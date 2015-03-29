# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
    This module provides a version of cassandra.concurrent.execute_concurrent
    which doesn't cause the statements_and_parameters to be materialized as a
    list. Also the results are provided back from a generator, so this
    implementation can execute statements in O(concurrency) instead of
    O(statements).

    This implementation is marginally slower on small numbers of statements but
    - if the results are not materialized together, i.e accumulated in a list -
    this implementation may take orders of magnitude less space and time for
    large numbers.
'''

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

try:
    from threading import _Semaphore as Semaphore
except ImportError:
    from threading import Semaphore
    
from heapq import heappush, heappop
from threading import Thread


class _JoinableSemaphore(Semaphore):
    '''
        Utility wrapper around threading.Sempaphore which allows 'joining',
        i.e. waiting for the semaphore to reach a certain value again.
    '''
    def join(self, value):
        ''' Blocks until the semaphore value is back at the given value '''
        if hasattr(self, '_cond'):
            self._join_py3(value)
        else:
            self._join_py2(value)
    
    def _join_py2(self, value):
        with self._Semaphore__cond:
            while self._Semaphore__value < value:
                self._Semaphore__cond.wait()

    def _join_py3(self, value):
        with self._cond:
            while self._value < value:
                self._cond.wait()


class _ConcurrentExecutor:
    '''
        This class implements the concurrent execution of statements, but
        please use the execute_concurrent(...) method for your convenience.
    '''

    def __init__(self, session, statements_and_parameters, concurrency,
                 raise_on_first_error, silent):

        self.session = session
        self.statements_and_parameters = statements_and_parameters
        self.concurrency = concurrency
        self.raise_on_first_error = raise_on_first_error
        self.silent = silent

        # the semaphore to ensure that no more than `concurrency` statements
        # are executed in parallel
        self.semaphore = _JoinableSemaphore(concurrency)

        # holder for any exception raised in executing the statements if
        # `raise_on_first_error` is set, execute_concurrent will raise the
        # value in this variable
        self.exception = None

        # queues to hold the results, one for as they come in and one in which
        # they are ordered by the index of the statement of the result
        self.results_unordered = Queue(concurrency * 2)
        self.results_ordered = Queue(concurrency * 2)

        # the index of the last statement, only set if last statement submitted
        self.last = None


    def results(self):
        ''' Generator which yields the results in statement order. '''

        # Start executing statements
        Thread(target=self._execute_statements, name='executor').start()

        if self.silent:
            # if in silent mode wait for final insert to be submitted
            while not self.last:
                self.semaphore.join(self.concurrency)
            # then wait for it to complete
            self.semaphore.join(self.concurrency)
        if not self.silent:
            # else, start ordering the results to return to the client
            Thread(target=self._order_results, name='orderer').start()
            return self._results_generator()

    def _results_generator(self):
        # yield ordered results until the sentinel value (idx == None) is found
        while True:
            if self.exception:
                # fail fast
                raise self.exception

            idx, succes, result = self.results_ordered.get()
            if idx == None:
                break
            else:
                yield succes, result

    def _execute_statements(self):
        '''
            Enumerates over the statements and parameters (a generator or an
            iterable) and, after acquiring a semaphore, asynchronously executes
            the statement. The call backs release the semaphore.
        '''

        for idx, (stmt, params) in enumerate(self.statements_and_parameters):
            # fail fast
            if self.exception and self.raise_on_first_error:
                break

            # wait for a slot to execute
            self.semaphore.acquire()

            # execute the statement asynchronously and set the call back
            future = self.session.execute_async(stmt, params)
            future.add_callback(self._succes, idx)
            future.add_errback(self._error, idx)

        # signal that all statements have been submitted
        self.last = idx


    def _done(self, result, idx, succes):
        ''' called when a statement was executed '''
        if not succes:
            self.failure = result

        if not self.silent:
            self.results_unordered.put((idx, succes, result))
            
        self.semaphore.release()

    def _succes(self, result, idx):
        ''' a statement was successfully executed '''
        self._done(result, idx, True)

    def _error(self, exception, idx):
        ''' a error occurred while executing a statement '''
        self._done(exception, idx, False)


    def _order_results(self):
        '''
            Orders the results from self.results_unorderd into
            self.results_ordered using a heap queue.
        '''

        # keep track of the next statement index to be emitted
        nxt = 0
        # buffer out of order results for later
        buf = []

        # loop until the last result has been emitted
        while not self.last or nxt < self.last:
            # fail fast
            if self.exception:
                raise self.exception

            # block until a result is available
            result = self.results_unordered.get()

            if result[0] != nxt:
                # if it is out of order, buffer it
                heappush(buf, result)
            else:
                # if it is 'in order' immediately emit it
                self.results_ordered.put(result)
                # and expect the next result
                nxt += 1

                # check the buffer if more results can be emitted
                while buf and buf[0][0] == nxt:
                    # if the first in the buffer is next, pop it and emit it
                    self.results_ordered.put(heappop(buf))
                    # and expect the next result
                    nxt += 1

        # emit a value indicating that no more results are coming
        self.results_ordered.put((None, None, None))




def execute_concurrent(session, statements_and_parameters, concurrency=10,
                       raise_on_first_error=True, silent=False):
    '''
        The returned results contains (success, result) tuples. Success
        is either True or False depending on whether the statement could be
        executed successfully or not. The result is the result of the statement
        or the exception raised executing it. The results are in the exact same
        order as the statements provided in `statements_and_parameters`.

        @param session: session to use for concurrently executing statements
        @param statements_and_parameters: the statements to execute and their
            parameters
        @param concurrency: the number of parallel statements to be in-flight
        @param raise_on_first_error: fail fast if set
        @param silent: whether to return results or not, if not silent, the
            results must be consumed, otherwise execution will halt
    '''

    if concurrency <= 0:
        raise ValueError("concurrency must be greater than 0")

    if not statements_and_parameters:
        return []

    executor = _ConcurrentExecutor(session, statements_and_parameters,
                                   concurrency, raise_on_first_error, silent)
    return executor.results()


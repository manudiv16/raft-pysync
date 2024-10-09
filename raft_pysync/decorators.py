import functools
import pickle
import sys
import threading
from typing import Callable, Any, Optional
import types
from raft_pysync import RaftPysyncObject
from raft_pysync.raft_pysync_obj import RaftPysyncObjectConsumer, RaftPysyncObjectException, CommandType


class AsyncResult(object):
    def __init__(self):
        self.result = None
        self.error = None
        self.event = threading.Event()

    def on_result(self, res, err):
        self.result = res
        self.error = err
        self.event.set()

def __copy_func(f, name):
    res = types.FunctionType(
        f.__code__, f.__globals__, name, f.__defaults__, f.__closure__
    )
    res.__dict__ = f.__dict__
    return res


import functools
import pickle
import sys
from typing import Callable, Optional, Any

def replicated(*decArgs, **decKwargs):
    """Replicated decorator. Use it to mark your class members that modifies
    a class state. Function will be called asynchronously. Function accepts
    flowing additional parameters (optional):
        'callback': callback(result, failReason), failReason - `FAIL_REASON <#pysyncobj.FAIL_REASON>`_.
        'sync': True - to block execution and wait for result, False - async call. If callback is passed,
            'sync' option is ignored.
        'timeout': if 'sync' is enabled, and no result is available for 'timeout' seconds -
            SyncObjException will be raised.
    These parameters are reserved and should not be used in kwargs of your replicated method.

    :param func: arbitrary class member
    :type func: function
    :param ver: (optional) - code version (for zero deployment)
    :type ver: int
    """
    def replicatedImpl(func):
        def newFunc(self, *args, **kwargs):

            if kwargs.pop('_doApply', False):
                return func(self, *args, **kwargs)
            else:
                if isinstance(self, RaftPysyncObject):
                    applier = self.apply_command
                    funcName = self.get_func_name(func.__name__)
                    funcID = self.method_to_id[funcName]
                elif isinstance(self, RaftPysyncObjectConsumer) and self._RaftPysyncObject:
                    consumerId = id(self)
                    funcName = self._RaftPysyncObject.get_func_name((consumerId, func.__name__))
                    funcID = self._RaftPysyncObject.method_to_id[(consumerId, funcName)]
                    applier = self._RaftPysyncObject.apply_command
                else:
                    raise RaftPysyncObjectException("Class should be inherited from SyncObj or SyncObjConsumer")

                callback = kwargs.pop('callback', None)
                if kwargs:
                    cmd = (funcID, args, kwargs)
                elif args and not kwargs:
                    cmd = (funcID, args)
                else:
                    cmd = funcID
                sync = kwargs.pop('sync', False)
                if callback is not None:
                    sync = False

                if sync:
                    asyncResult = AsyncResult()
                    callback = asyncResult.on_result

                    timeout = kwargs.pop('timeout', None)
                    applier(pickle.dumps(cmd), callback, CommandType.REGULAR)

                    res = asyncResult.event.wait(timeout)
                    if not res:
                        raise RaftPysyncObjectException('Timeout')
                    if not asyncResult.error == 0:
                        raise RaftPysyncObjectException(asyncResult.error)
                    return asyncResult.result

                else:
                    timeout = kwargs.pop('timeout', None)
                    applier(pickle.dumps(cmd), callback, CommandType.REGULAR)

        func_dict = newFunc.__dict__
        func_dict['replicated'] = True
        func_dict['ver'] = int(decKwargs.get('ver', 0))
        func_dict['origName'] = func.__name__

        callframe = sys._getframe(1 if decKwargs else 2)
        namespace = callframe.f_locals
        newFuncName = func.__name__ + '_v' + str(func_dict['ver'])
        namespace[newFuncName] = __copy_func(newFunc, newFuncName)
        functools.update_wrapper(newFunc, func)
        return newFunc

    if len(decArgs) == 1 and len(decKwargs) == 0 and callable(decArgs[0]):
        return replicatedImpl(decArgs[0])

    return replicatedImpl

def replicated_sync(*decArgs, **decKwargs):
    def replicated_sync_impl(func, timeout = None):
        """Same as replicated, but synchronous by default.

        :param func: arbitrary class member
        :type func: function
        :param timeout: time to wait (seconds). Default: None
        :type timeout: float or None
        """

        def newFunc(self, *args, **kwargs):
            if kwargs.get('_doApply', False):
                return replicated(func)(self, *args, **kwargs)
            else:
                kwargs.setdefault('timeout', timeout)
                kwargs.setdefault('sync', True)
                return replicated(func)(self, *args, **kwargs)
        func_dict = newFunc.__dict__ 
        func_dict['replicated'] = True
        func_dict['ver'] = int(decKwargs.get('ver', 0))
        func_dict['origName'] = func.__name__

        callframe = sys._getframe(1 if decKwargs else 2)
        namespace = callframe.f_locals
        newFuncName = func.__name__ + '_v' + str(func_dict['ver'])
        namespace[newFuncName] = __copy_func(newFunc, newFuncName)
        functools.update_wrapper(newFunc, func)
        return newFunc

    if len(decArgs) == 1 and len(decKwargs) == 0 and callable(decArgs[0]):
        return replicated_sync_impl(decArgs[0])

    return replicated_sync_impl

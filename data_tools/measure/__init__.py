import time


def _collect_time(create_entry):
    def wrapper(f):
        def _wrapper(*args, **kwargs):
            start_time = time.time_ns()
            resp = f(*args, **kwargs)
            spent_time = time.time_ns() - start_time
            if not args[0].explain:
                args[0].times.append(create_entry(*args, **kwargs, resp=resp, spent_time=spent_time))
            return resp
        return _wrapper
    return wrapper

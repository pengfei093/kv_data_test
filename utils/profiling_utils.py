# import time
# from functools import wraps
# import memory_profiler as mem_profile
#
#
# def timefn(fn):
#     @wraps(fn)
#     def measure_time(*args, **kwargs):
#         t1 = time.time()
#         result = fn(*args, **kwargs)
#         t2 = time.time()
#         print(f"@timefn: {fn.__name__} took {t2 - t1} seconds")
#         return result
#
#     return measure_time


# def memoryfn(fn):
#     @wraps(fn)
#     def measure_memory(*args, **kwargs):
#         print(f"@memoryfn Beofre: {fn.__name__} took {mem_profile.memory_usage()} MB")
#         result = fn(*args, **kwargs)
#         print(f"@memoryfn After: {fn.__name__} took {mem_profile.memory_usage()} MB")
#         return result
#
#     return measure_memory

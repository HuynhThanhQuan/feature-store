import time
from functools import wraps
import logging

logger = logging.getLogger(__name__)

def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.debug(f'[timeit] Function "{func.__name__}" took "{elapsed_time}" seconds')
        return result
    return wrapper


@timeit
def my_func(n=100):
    for _ in range(n):
        pass
    
if __name__ == '__main__':    
    my_func()

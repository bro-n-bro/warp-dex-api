def get_first_if_exists(func):
    def wrapper(*args, **kwargs):
        list_of_items = func(*args, **kwargs)
        if len(list_of_items):
            return list_of_items[0]
        else:
            return None
    return wrapper

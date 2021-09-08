import random
import string
import uuid


def generate_uuid(db_safe=False):
    if db_safe:
        return generate_random_password(40)
    return str(uuid.uuid4())


def generate_random_password(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str

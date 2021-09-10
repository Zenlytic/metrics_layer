def query(
    metrics: list, dimensions: list = [], where: list = [], having: list = [], order_by: list = [], raw=False
):
    raise NotImplementedError()


def define(metric: str):
    raise NotImplementedError()

import gc
import weakref

from metrics_layer.core.utils import clear_instance_memo, instance_memoize


class Counter:
    def __init__(self):
        self._instance_memo = {}
        self.calls = 0

    @instance_memoize
    def compute(self, x, y=0):
        self.calls += 1
        return [x, y]  # a fresh mutable so identity checks are meaningful


def test_memoizes_repeat_calls_same_args():
    c = Counter()
    a = c.compute(1)
    b = c.compute(1)
    assert a is b
    assert c.calls == 1


def test_distinguishes_positional_from_keyword_like_lru_cache():
    c = Counter()
    c.compute(1)
    c.compute(1, y=0)  # different key shape than compute(1), like functools.lru_cache
    assert c.calls == 2


def test_distinct_args_recompute():
    c = Counter()
    c.compute(1)
    c.compute(2)
    assert c.calls == 2


def test_lazy_init_without_declared_memo():
    class Bare:
        @instance_memoize
        def f(self, x):
            return {"x": x}

    b = Bare()
    assert b.f(5) is b.f(5)


def test_clear_all():
    c = Counter()
    first = c.compute(1)
    clear_instance_memo(c)
    second = c.compute(1)
    assert first is not second
    assert c.calls == 2


def test_clear_named_only():
    class Two:
        def __init__(self):
            self._instance_memo = {}

        @instance_memoize
        def a(self):
            return object()

        @instance_memoize
        def b(self):
            return object()

    t = Two()
    a1, b1 = t.a(), t.b()
    clear_instance_memo(t, "a")
    assert t.a() is not a1
    assert t.b() is b1


def test_clear_tolerates_missing_memo():
    class Bare:
        pass

    clear_instance_memo(Bare())  # must not raise


def test_cache_dies_with_instance():
    c = Counter()
    c.compute(1)
    ref = weakref.ref(c)
    del c
    gc.collect()
    assert ref() is None  # the instance is not retained by any class-level structure

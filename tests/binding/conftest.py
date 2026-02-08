"""Helpful fixtures for testing dinao.binding functionality."""

from dinao.binding import AsyncFunctionBinder, FunctionBinder

import pytest

from tests.binding.mocks import AsyncMockConnectionPool, MockConnectionPool


@pytest.fixture()
def binder_and_pool(request):
    """Fixture that yields a FunctionBinder (and its MockedConnectionPool) initialized with a set of MockResultSets.

    .. note::
        Can use the `indirect` parametrize functionality in fixture to specify the mocked results.
    """
    result_stack = []
    if hasattr(request, "param"):
        result_stack = request.param
    pool = MockConnectionPool(result_stack)
    binder = FunctionBinder()
    binder.pool = pool
    yield binder, pool
    pool.dispose()


@pytest.fixture()
def async_binder_and_pool(request):
    """Fixture that yields an AsyncFunctionBinder (and its AsyncMockConnectionPool) with a set of MockResultSets.

    .. note::
        Can use the `indirect` parametrize functionality in fixture to specify the mocked results.
    """
    result_stack = []
    if hasattr(request, "param"):
        result_stack = request.param
    pool = AsyncMockConnectionPool(result_stack)
    binder = AsyncFunctionBinder()
    binder.pool = pool
    yield binder, pool

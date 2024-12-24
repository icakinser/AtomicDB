import pytest
from atomicdb import AtomicDB
from atomicdb.results import QueryResult

@pytest.fixture
def sample_docs():
    """Sample documents for testing."""
    return [
        {"_id": 0, "name": "Alice", "age": 30},
        {"_id": 1, "name": "Bob", "age": 25},
        {"_id": 2, "name": "Charlie", "age": 35}
    ]

def test_query_result_creation(sample_docs):
    """Test QueryResult creation and basic list operations."""
    result = QueryResult(sample_docs)
    assert len(result) == 3
    assert result[0]["name"] == "Alice"
    assert result[-1]["name"] == "Charlie"

def test_query_result_iteration(sample_docs):
    """Test iteration over QueryResult."""
    result = QueryResult(sample_docs)
    names = [doc["name"] for doc in result]
    assert names == ["Alice", "Bob", "Charlie"]

def test_query_result_slicing(sample_docs):
    """Test slicing QueryResult."""
    result = QueryResult(sample_docs)
    slice_result = result[1:3]
    assert len(slice_result) == 2
    assert isinstance(slice_result, QueryResult)
    assert [doc["name"] for doc in slice_result] == ["Bob", "Charlie"]

def test_first_last(sample_docs):
    """Test first() and last() methods."""
    result = QueryResult(sample_docs)
    assert result.first()["name"] == "Alice"
    assert result.last()["name"] == "Charlie"
    
    empty_result = QueryResult([])
    assert empty_result.first() is None
    assert empty_result.last() is None

def test_pluck(sample_docs):
    """Test pluck() method."""
    result = QueryResult(sample_docs)
    names = result.pluck("name")
    assert len(names) == 3
    assert all("age" not in doc for doc in names)
    assert all("name" in doc for doc in names)
    assert names[0]["name"] == "Alice"

def test_exclude(sample_docs):
    """Test exclude() method."""
    result = QueryResult(sample_docs)
    no_age = result.exclude("age")
    assert len(no_age) == 3
    assert all("age" not in doc for doc in no_age)
    assert all("name" in doc for doc in no_age)

def test_update_all(sample_docs):
    """Test update_all() method."""
    result = QueryResult(sample_docs)
    result.update_all({"country": "USA"})
    assert all("country" in doc for doc in result)
    assert all(doc["country"] == "USA" for doc in result)

def test_as_list(sample_docs):
    """Test as_list() method."""
    result = QueryResult(sample_docs)
    list_result = result.as_list()
    assert isinstance(list_result, list)
    assert len(list_result) == 3
    assert list_result == sample_docs

def test_count_and_is_empty(sample_docs):
    """Test count() and is_empty() methods."""
    result = QueryResult(sample_docs)
    assert result.count() == 3
    assert not result.is_empty()
    
    empty_result = QueryResult([])
    assert empty_result.count() == 0
    assert empty_result.is_empty()

def test_sort_by(sample_docs):
    """Test sort_by() method."""
    result = QueryResult(sample_docs)
    
    # Sort by age ascending
    sorted_asc = result.sort_by("age")
    ages = [doc["age"] for doc in sorted_asc]
    assert ages == [25, 30, 35]
    
    # Sort by age descending
    sorted_desc = result.sort_by("age", reverse=True)
    ages = [doc["age"] for doc in sorted_desc]
    assert ages == [35, 30, 25]

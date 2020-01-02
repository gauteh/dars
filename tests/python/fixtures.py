import os
import pytest

@pytest.fixture
def dars():
  return "http://localhost:8001/data/"

@pytest.fixture
def tds():
  return "http://localhost:8002/thredds/dodsC/test/data/"

@pytest.fixture
def data():
  return os.path.join(os.path.dirname(__file__), "../../data/")


.PHONY: 
utest:
	PYTHONPATH=. pytest

test: utest

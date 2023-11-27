.PHONY: 

install:
	pip install -r requirements.txt

utest:
	PYTHONPATH=. pytest \
		--disable-warnings \
		--disable-pytest-warnings

test: utest

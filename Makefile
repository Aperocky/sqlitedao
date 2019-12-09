build:
	@python3 -m pip install .

test:
	@pytest

dist:
	@python3 setup.py sdist bdist_wheel

upload_package:
	@twine upload dist/* --skip-existing

.PHONY: build test dist

build:
	@python3 -m pip install .

test:
	@coverage run -m pytest
	@coverage report --omit=/usr/*,test* -m

dist:
	@python3 setup.py sdist bdist_wheel

upload_package:
	@twine upload dist/* --skip-existing

.PHONY: build test dist

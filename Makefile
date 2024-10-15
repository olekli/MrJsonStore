.PHONY: test format

default: format test

format:
	poetry run black \
		--skip-string-normalization \
		--line-length 100 \
			mrjsonstore test

test:
	poetry run pytest
	poetry run mypy mrjsonstore test \
		--enable-incomplete-feature=NewGenericSyntax

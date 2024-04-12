## [New Motoko Library]
This template is a version of [motoko-library-template](https://github.com/kritzcreek/motoko-library-template) that uses the [motoko-unit-tests](https://github.com/krpeacock/motoko-unit-tests) library in the [tests/utils](./tests/utils/ActorSpec.mo) directory

### Makefile Commands
- `make test` 
  - runs your motoko tests by interpreting the code with the motoko compiler
  - Tests files have to be in the `/tests` directory and end with `.Test.mo`
- `make doc` 
  - creates html and markdown documentation from your inline comments (comments starting with 3 backslashes `///`)

### Github Actions
- Actions for running tests every time there is a push or pull request to the `main` branch
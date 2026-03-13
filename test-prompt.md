Write unit tests for the following rust file. Follow these rules:

1. use pretty_assertions instead of the standard one
2. write tests that will fail with clear error (e.g., use `actual.unwrap()` instead of `assert!(result.is_ok()`)
3. avoid unit tests that test too much, prefer tests that test small piece of functionality
4. write high fidelity unit tests, avoid mocking if possible
5. focus on the functionality that a particular module exposes, like a exposed function or method
6. cover all the bases when writing cases


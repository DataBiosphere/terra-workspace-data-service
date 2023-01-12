The classes under `bio.terra.common` are copied, as source code, from
the `terra-common-lib` [github repository](https://github.com/DataBiosphere/terra-common-lib).

We do this because importing `terra-common-lib` as a whole has side effects such as changing
logging config, but we want the `@ReadTransaction` and `@WriteTransaction` annotation support.

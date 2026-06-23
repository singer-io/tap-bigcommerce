# Changelog

## 1.3.0
  * Exclude 403-forbidden streams from discovery instead of failing [#25](https://github.com/singer-io/tap-bigcommerce/pull/25)
  * Adds unit tests for discovery access checks.
  * Upgrade `requests` from 2.33.1 to 2.34.2.

## 1.2.0
  * Updated python version 3.12 [#24](https://github.com/singer-io/tap-bigcommerce/pull/24)
  * Upgraded dependancies
  * Added integration tests.

## 1.1.4
  * Bump requests to 2.32.4 [#22](https://github.com/singer-io/tap-bigcommerce/pull/22)

## 1.1.3
  * Dependabot update [#19](https://github.com/singer-io/tap-bigcommerce/pull/19)

## 1.1.2
  * Change orders schema to allow string as well as integer at JSON path `products.[].external_id` [#13](https://github.com/singer-io/tap-bigcommerce/pull/13)

## 1.1.1
  * Reverts previous version [#10](https://github.com/singer-io/tap-chargebee/pull/10)

## 1.1.0
  * Adds `Customersv3` and `AttributeValues` streams [#7](https://github.com/singer-io/tap-chargebee/pull/7)

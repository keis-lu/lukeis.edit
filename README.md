
<!-- README.md is generated from README.Rmd. Please edit that file -->

# lukeis.edit

<!-- badges: start -->
<!-- badges: end -->

The goal of lukeis.edit is to apply patches on LU tables.

## Installation

You can install the development version of lukeis.edit from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("keis-lu/lukeis.edit")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(lukeis.edit)
con <- keis.base::keis_con("harmonized_prod")
patch_pv_bfsnr_values(con, bfsnr_to = 1061L)
```

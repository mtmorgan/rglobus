# rglobus

<!-- badges: start -->
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

'Globus' is a cloud-based file transfer service, available at
<https://www.globus.org/>. This package provides an 'R' client with
the ability to discover and navigate file collections, and to transfer
files and directories between collections. Use of the package is
illustrated with data from the 'HuBMAP'
<https://portal.hubmapconsortium.org/> data portal and 'HuBMAPR'
package.


## Installation

Install rglobus from [GitHub](https://github.com/mtmorgan) with:

``` r
if (!requireNamespace("remotes", quiety = TRUE))
    install.packages("remotes", repos = "https://CRAN.R-project.org")
remotes::install_github("mtmorgan/rglobus")
```

See the [getting started][] vignette for an introduction to
configuration and use.

[getting started]: https://mtmorgan.github.io/rglobus/articles/a_getting_started.html

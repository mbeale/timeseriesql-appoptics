<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![LinkedIn][linkedin-shield]][linkedin-url]


<!-- PROJECT LOGO -->
<br />
<p align="center">
  <h3 align="center">TimeSeriesQL</h3>

  <p align="center">
    A Pythonic query language for time series data
  </p>
</p>



<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [Roadmap](#roadmap)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)



<!-- ABOUT THE PROJECT -->
## About The Project

This project add AppOptics as a backend for the TimeSeriesQL ecosystem.    


### Built With

* [TimeSeriesQL](https://github.com/mbeale/timeseriesql)
* [requests](https://requests.readthedocs.io/en/master/)



<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.

### Prerequisites

The requirements are in the [requirements.txt](requirements.txt) file.

### Installation

#### pip

```sh
pip install timeseriesql-appoptics
```

#### manual

1. Clone the timeseriesql-appoptics
```sh
git clone https:://github.com/mbeale/timeseriesql-appoptics.git
```
2. Install library
```sh
cd timeseriesql-appoptics
python setup.py install 
```

<!-- USAGE EXAMPLES -->
## Usage

[Appoptics](www.appoptics.com) is a commercial time series database product.  The backend converts a query into an 
API call.

The backend expects a ``APPOPTICS_TOKEN`` environment variable to be set in order to authenticate to AppOptics.

#### AppOptics Query

```python
from timeseriesql_appoptics import AOBackend

data = AOBackend(x for x in "metric.name")['1h'] #basic
data = AOBackend(x * 100 for x in "metric.name")['1h'] #binary operations (+, -, /, *)
data = AOBackend(x * 1.8 + 32 for x in "metric.name")['1h'] #multiple binary operations (°C to °F)
data = AOBackend(x.max for x in "metric.name")[3600:] #get max value
```

#### AppOptics Filtering

Currently only ``==`` and ``!=`` are supported.

```python
from timeseriesql_appoptics import AOBackend

data = AOBackend(x for x in "metric.name" if x.environment == 'production')[3600:]
```

#### AppOptics Grouping

```python
from timeseriesql_appoptics import AOBackend

data = AOBackend(x for x in "metric.name").group('environment')[3600:]
data = AOBackend(x - y for x,y in AOBackend((x.max for x in "metric1"), (x.min for x in "metric2")).by('tag1'))[3600:]
```

#### AppOptics Time
```python
from timeseriesql_appoptics import AOBackend

data = AOBackend(x for x in "metric.name")[:] #no start or end time (not recommended)
data = AOBackend(x for x in "metric.name")[3600:] #from now - 3600 seconds until now, resolution of 1
data = AOBackend(x for x in "metric.name")[3600:1800] #from now - 3600 seconds until now - 1800 seconds, resolution of 1
data = AOBackend(x for x in "metric.name")[3600::300] #from now - 3600 seconds until now resoultion of 300 seconds
```

#### AppOptics Functions
```python
data = AOBackend(sum(derive(x)) for x in "metric.name")[3600:] #get the sums of the derivatives
data = AOBackend(zero_fill(x) for x in "metric.name")[3600::60] #zero_fill
```

#### AppOptics Raw Composite
```python
data = AOBackend('s("some_metric", "*")')[3600:]
```

<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/mbeale/timeseriesql-appoptics/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

Michael Beale - michael.beale@gmail.com

Project Link: [https://github.com/mbeale/timeseriesql-appoptics](https://github.com/mbeale/timeseriesql-appoptics)



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/mbeale/timeseriesql-appoptics.svg?style=flat-square
[contributors-url]: https://github.com/mbeale/timeseriesql-appoptics/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/mbeale/timeseriesql-appoptics.svg?style=flat-square
[forks-url]: https://github.com/mbeale/timeseriesql-appoptics/network/members
[stars-shield]: https://img.shields.io/github/stars/mbeale/timeseriesql-appoptics.svg?style=flat-square
[stars-url]: https://github.com/mbeale/timeseriesql-appoptics/stargazers
[issues-shield]: https://img.shields.io/github/issues/mbeale/timeseriesql-appoptics.svg?style=flat-square
[issues-url]: https://github.com/mbeale/timeseriesql-appoptics/issues
[license-shield]: https://img.shields.io/github/license/mbeale/timeseriesql-appoptics.svg?style=flat-square
[license-url]: https://github.com/mbeale/timeseriesql-appoptics/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/michael-beale-163a4670

# o3-flux
This repository includes the processing scripts to calculate O3 fluxes and concentrations from the fast EC system and the slow O3 monitor.

One year of O3 fluxes (2025) has already been published here: https://doi.org/10.17887/WUR01-6UGU9F

The scripts are serving the following purpose:
- Loobos_O3_Calibration.py: Calibrates the Ozone fluxes and produces the calibrated fluxes on the Yoda research drive.
- Loobos_O3_MakeYearFile.py: Combines day files to produce one-year file. These files are not (yet) included in the publication of the datasets.
- Loobos_O3_ProcessFluxes.py: Operational script that processes the fluxes and produces e.g. the figure on https://met.wur.nl/loobos/graphs/cur/.

All scripts above use the Loobos_Toolbox_NewTower.py where the main functions are located (e.g. reading in the files and producing figures).
Please note that the scripts rely on access to the folders where the (raw) input data is located on the WUR enterprise drive.

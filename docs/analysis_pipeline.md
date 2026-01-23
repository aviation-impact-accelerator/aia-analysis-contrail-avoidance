# Analysis pipeline for contrail avoidance modelling

This document outlines the analysis pipeline for contrail avoidance modeling using the provided tools and datasets. The pipeline includes data preparation, model execution, and result analysis.

## Data Preparation

1. **CoCIP Grid Generation**: Use the `generate_cosip_grid_environment.py` script to create a CoCIP grid environment.
   Specify the required spatial and temporal bounds, pressure levels, and output filename as described in the [Creating a CoCIP Grid Environment for Contrail Modeling](creating_cosip_grid.md) document.
   For this you will require meteorological data downloaded from the CDS.
   Ensure that you have registered for a CDS account and have your API key set up in `~/.cdsapirc`.

2. **ADS-B Flight Data Processing**: Use the `process_adsb_data.py` script to extract relevant flight paths and times for contrail modeling.
   More information on the steps taken to process the ADS-B data can be found in the [Processing ADS-B Data Documentation](ads_b_data_processing.md).

## Model Execution

1. **Run Contrail Avoidance Model**: Use the `calculate_energy_forcing.py` script to execute the contrail avoidance model using the generated CoCIP grid and processed flight data. This step involves simulating contrail formation and generating a file of statistics related to contrail avoidance.

## Result Analysis

1. **Visualize Results**: Use visualization tools to analyze the output of the contrail avoidance model.
   This may include plotting the temporal and spatial variation of air traffic density and contrail formation risk.
   Scripts for visualization can be found in the `analysis` folder.

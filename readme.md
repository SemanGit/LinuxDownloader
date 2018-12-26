
# LinuxDownloader
This repository is used to develop the LinuxDownloader of the SemanGit Project. To use it download the zip and execute ```./LinuxDownloader```
For the main project repository, please visit the [SemanGit](https://github.com/SemanGit/SemanGit) repository

# Usage
The Downloader will guide you through all important steps for processing and will ensure fault tolerant generation of the dataset.
The steps include:

1. Confirmation of License Agreements
2. Installation of Dependencies (curl, pigz, jre, zipper)
3. Selection of Datasets to generate
4. Download (automatic)
5. Unzipping (automatic)
6. Conversion (automatic)
7. Merging Dataset (automatic)


# Options
Options include:
- **-h, -help** print the help file
- **-skip_install** - skips the depencency checks and the installation of packages. 
- **-keep_everything** - keeps the output of all intermediate steps (4-7). (Attention: This results in a huge storage overhead)
- **-output_dir=<absolute_path>** - change the storage location for all steps and the final output
- **-converter_options=&quot;-option1 -option2 ... &quot;** - passes options to the converter. A list of available options can be found in the folder Converter

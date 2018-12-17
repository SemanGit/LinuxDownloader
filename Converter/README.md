# Converter
This repository is used to develop the Java converter of the SemanGit Project. The compiled .jar file as well as the main class can be found in the main repository as well.
For the main project repository, please visit the [SemanGit](https://github.com/SemanGit/SemanGit) repository

The converter was formerly developed in a different repository, which is no longer in use. It can be found here: https://github.com/Madmatti/semangit_intellij

# Usage
You can run the converter on an extracted dataset from [GHTorrent](www.ghtorrent.org) as follows:
```sh
java -jar converter_main.jar path/to/extracted/files \[options\]
```

# Options
Options include:
- **-sampling=random** - allows for random sampling. Triples are randomly accepted or rejected based on percentage parameter. If this parameter is not given, no sampling is done. 
- **-percentage=X** - for X in (0, 1\], this determines with what probability a triple is accepted during random sampling.
- **-nomerging** - does not create a combined.ttl file, but instead keeps one file per input csv file.
- **-nostrings**: does not include user comments in output. This can be used to create a cleaner output that is easier to parse by another application.
- **-noblank**: forces the converter to not create any blank nodes, but assign unique identifiert for all entities. This option is suitable for loading in the data in key value store.
###### For internal evaluation:
- **-base=10|16|32|64** - changes the integer string representation. Default is 64. This uses the Base64URL approach, except for the minus sign missing. This would violate to the Turtle Syntax, if it occurs as leading character.
- **-noprefix** - experimental parameter used for performance tests on the compression. This enforces base=10 though and does not use turtles prefixing capabilities. This will result in a highly expanded dataset of possibly several TB

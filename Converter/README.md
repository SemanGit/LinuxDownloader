# Converter
This repository is used to develop the Java converter of the SemanGit Project. The compiled .jar file as well as the main class can be found in the main repository as well.
For the main project repository, please visit https://github.com/SemanGit/SemanGit

The converter was formerly developed in a different repository, which is no longer in use. It can be found here: https://github.com/Madmatti/semangit_intellij/

You can run the converter on an extracted dataset from GHTorrent as follows:

java -jar converter_main.jar path/to/extracted/files \[options\]

Options include:

-base=10|16|32|64 - changes the integer string representation. Default is 64. This uses the Base64URL approach, except for the minus sign missing. This causes issues if it occurs as leading character.

-noprefix - an experimental parameter used for statistical purposes. This enforces base=10 though and does not use turtles prefixing capabilities.

-sampling=random - allows for random sampling. Triples are randomly accepted or rejected based on percentage parameter. If this parameter is not given, no sampling is done.

-percentage=X - for X in (0, 1\], this determines with what probability a triple is accepted during random sampling.

-nomerging - does not create a combined.ttl file, but instead keeps one file per input csv file.

-nostrings - does not include user comments in output. This can be used to create a cleaner output that is easier to parse by another application.

-noblank - forces the converter to not create any blank nodes, but assign unique identifiert for all entities. We use this for some internal evaluations in a key value store.

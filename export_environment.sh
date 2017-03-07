conda env create -f environment.yml
rm -rf memexeval2017
conda create -m -p $(pwd)/memexeval2017/ --copy --clone memexeval2017
zip -r memexeval2017.zip memexeval2017

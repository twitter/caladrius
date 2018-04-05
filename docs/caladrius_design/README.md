# Caladrius - Design Document

The design document is written in 
[Pandoc flavoured Markdown](https://pandoc.org/MANUAL.html#pandocs-markdown).

To compile it to a pdf first make sure you have following packages installed:

* `pandoc`
* `pandoc-crossref`
* `pdflatex`

These should be available through your OS package manager. Then run the
following command (change `output/main.pdf` to your desired output):

    pandoc --from markdown --to latex -o output/main.pdf \
        --filter pandoc-crossref --number-sections \
        *.md meta.yaml


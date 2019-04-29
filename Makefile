# Convert the source files into easier-to-read versions...
#
easy-to-read:
	mkdir -p ./tmp/easy-to-read
	rm -f ./tmp/easy-to-read/*.go
	for f in ./*.go; do \
       sed -e 's/[Ll]z//g' $$f | sed -e 's/ \/\/ !//g' | sed -e 's/ \/\/ <== .*//g' > ./tmp/easy-to-read/$$f; \
    done

JBUILDER ?= jbuilder

example: 
	$(JBUILDER) build --dev @install

clean:
	rm -rf _build


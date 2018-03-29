JBUILDER ?= jbuilder

example: 
	$(JBUILDER) build @install

clean:
	rm -rf _build


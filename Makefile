EXTENSION = hypopg
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config

MODULE_big = hypopg
OBJS = hypopg.o hypopg_import.o hypopg_index.o hypopg_table.o

all:

release-zip: all
	git archive --format zip --prefix=hypopg-${EXTVERSION}/ --output ./hypopg-${EXTVERSION}.zip HEAD
	unzip ./hypopg-$(EXTVERSION).zip
	rm ./hypopg-$(EXTVERSION).zip
	rm ./hypopg-$(EXTVERSION)/.gitignore
	rm ./hypopg-$(EXTVERSION)/typedefs.list
	rm ./hypopg-$(EXTVERSION)/TODO.md
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./hypopg-$(EXTVERSION)/META.json
	zip -r ./hypopg-$(EXTVERSION).zip ./hypopg-$(EXTVERSION)/
	rm ./hypopg-$(EXTVERSION) -rf


DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

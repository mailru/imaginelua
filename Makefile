.PHONY: own

own:
	@\
	\
	read -p "Enter project name: " project; \
	echo "$$project" | perl -lne '/^[a-z]+$$/ or die "Bad format\n"'; \
	\
	mkdir -p dist; \
	sed "s/swa\.tnt/$$project.tnt/" lua/imagine.lua > dist/imagine.lua; \
	cp lua/graphite.lua dist/graphite.lua; \
	sed "s/swa\(.\)tnt/$$project\1tnt/" grafana/dashboard.json > dist/dashboard.json; \
	\
	echo 'Done. Saved to dist/'

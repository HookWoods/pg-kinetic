compat:
	@cc $$(pkg-config --cflags libpq) compat.c $$(pkg-config --libs libpq) -o compat-libpq
	@./compat-libpq

clean:
	@rm -f compat-libpq

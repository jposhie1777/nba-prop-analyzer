def chunked(items, size=25):
    for i in range(0, len(items), size):
        yield items[i:i + size]
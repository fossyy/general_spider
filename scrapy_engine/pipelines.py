import json

class JsonWriterPipeline:
    def open_spider(self, spider):
        output_file = getattr(spider, 'output_file', None)
        if not output_file:
            raise ValueError('output_file must be specified')

        self.file = open(output_file, 'w')
        self.file.write('[')
        self.first_item = True

    def close_spider(self, spider):
        self.file.write(']')
        self.file.close()

    def process_item(self, item, spider):
        if not self.first_item:
            self.file.write(',\n')
        else:
            self.first_item = False

        line = json.dumps(dict(item), indent=None)
        self.file.write(line)
        return item
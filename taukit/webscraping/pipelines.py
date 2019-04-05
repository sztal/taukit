"""Scrapy pipeline classes."""
# pylint: disable=unused-argument


class TauItemPipeline:
    """Item pipeline for persisting spiders' output."""

    def __init__(self):
        """Initialization method."""
        self.db = None
        self.disk = None

    def open_spider(self, spider):
        if spider.storage.lower() != 'no':
            self.setup_disk_persister(spider)
        if spider.storage.lower() not in ('no', 'none', 'nodb'):
            self.setup_db_persister(spider)

    def setup_disk_persister(self, spider):
        raise NotImplementedError

    def setup_db_persister(self, spider):
        raise NotImplementedError

    def process_item(self, item, spider):
        # pylint: disable=unused-argument
        if self.disk:
            self.disk.persist(item)
        return item

    def close_spider(self, spider):
        if self.db:
            items = self.disk.load()
            self.db.persist_many(items)

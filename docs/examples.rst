examples
-------------------------------

Update all documents with the 'hamburger' tag. ::

    from sist2 import Sist2Index
    import sys

    index = Sist2Index(sys.argv[1])
    for doc in index.document_iter():
        doc.json_data["tag"] = ["hamburger.#00FF00"]
        index.update_document(doc)

    index.sync_tag_table()
    index.commit()

    print("Done!")
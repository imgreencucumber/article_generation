#!/usr/bin/env python3
# pylint: disable=import-outside-toplevel

from __future__ import annotations
from yandex_cloud_ml_sdk import YCloudML

doc_texts = [
    """Александр Сергеевич Пушкин (26 мая [6 июня] 1799, Москва — 29 января [10 февраля] 1837, Санкт-Петербург)
    — русский поэт, драматург и прозаик, заложивший основы русского реалистического направления,
    литературный критик и теоретик литературы, историк, публицист, журналист.""",
    """Ромашка — род однолетних цветковых растений семейства астровые,
    или сложноцветные, по современной классификации объединяет около 70 видов невысоких пахучих трав,
    цветущих с первого года жизни.""",
]
query_text = "когда день рождения Пушкина?"


def main():
    import numpy as np
    from scipy.spatial.distance import cdist

    sdk = YCloudML(
        folder_id="b1gg1kh7b3mtup23kjbd",
        auth="t1.9euelZqXjZGYzpuUx8eclZydjJKXz-3rnpWalZKaksaezc-alpmdzsqem4rl8_djPg5B-e8XGkh7_N3z9yNtC0H57xcaSHv8zef1656VmpnKi5KTncyejsmLm4vPm52J7_zN5_XrnpWazcfHnYqUm5TJkMqWzZWUi4zv_cXrnpWamcqLkpOdzJ6OyYubi8-bnYk.wwUvTghk8oN_a-xf-dUA7qFw-erOsm21LVGt6FtAtWK8rXNImM4aFDHQy5RlAQKX-ajaHoRRAIuQekuafg2KCg",
    )

    query_model = sdk.models.text_embeddings("query")
    query_embedding = query_model.run(query_text)

    doc_model = sdk.models.text_embeddings("doc")
    doc_embeddings = [doc_model.run(text) for text in doc_texts]

    query_embedding = np.array(query_embedding)

    dist = cdist([query_embedding], doc_embeddings, metric="cosine")
    sim = 1 - dist
    result = doc_texts[np.argmax(sim)]
    print(result)


if __name__ == "__main__":
    main()

# Query Service – Flow (In-Depth, Simplified)

This document describes **what happens when you query**: which files run, in what order, and what each part does. The “query service” is the **Python FastAPI app** that handles search and chat; the Node.js app proxies requests to it.

---

## 1. Entry Points & Routing

### 1.1 How a request reaches the query service

- **Node.js** (e.g. `backend/nodejs/apps/src/modules/enterprise_search/controller/es_controller.ts`) receives the user request.
- It forwards to the **Python query service** using `appConfig.aiBackend` (from `paths.ts`: `aiBackend: '/services/query'`). Resolved URL is the query service base (e.g. `http://query-service:8000`).
- **Python app** is started from `backend/python/app/query_main.py`; the FastAPI `app` mounts:
  - `POST /api/v1/search` → **search** (semantic search only)
  - `POST /api/v1/chat` → **chat** (search + LLM answer)
  - `POST /api/v1/chat/stream` → **chat stream**
  - `POST /api/v1/agent/...` → **agent** (LangGraph agent chat)

So “query service” = this Python app; “query flow” = one of these endpoints.

### 1.2 Dependency injection (container)

- **File:** `backend/python/app/containers/query.py`
- **Role:** Builds and wires the query app:
  - `config_service`, `arango_client`, `redis_client`, `arango_service`, `vector_db_service`, `blob_store`, `retrieval_service`, `reranker_service`.
- **Startup:** In `query_main.py`, `lifespan()` calls `get_initialized_container()`, which:
  - Runs `initialize_container()` (connector health check, Arango init).
  - Calls `container.wire(modules=[...])` so routes get injected services.
- The container is stored on `app.container` and used by route dependencies (`get_retrieval_service`, `get_arango_service`, etc.) to resolve services per request.

---

## 2. Flow A: POST /api/v1/search (Semantic search only)

**File:** `backend/python/app/api/routes/search.py`

**Steps:**

1. **Auth**  
   `query_main.py` middleware runs `authMiddleware(request)`; `request.state.user` gets `userId`, `orgId`.

2. **Route handler:** `search(body: SearchQuery, ...)`  
   - Reads `query`, `limit`, `filters` from body.
   - Gets `retrieval_service` and `arango_service` from container (via `get_retrieval_service`, `get_arango_service`).

3. **KB filter (if any)**  
   - If `body.filters` has `kb` (knowledge base IDs):
     - `arango_service.validate_user_kb_access(user_id, org_id, kb_ids)` → allowed KBs.
     - If none allowed → 403 and exit.
     - Else `updated_filters['kb'] = accessible_kbs`.

4. **Query transformation (LLM)**  
   - `setup_query_transformation(llm)` from `app/utils/query_transform.py` returns two chains:
     - **Rewrite:** one rewritten, more specific query.
     - **Expansion:** 2 extra queries (different aspects).
   - `rewrite_chain.ainvoke(body.query)` and `expansion_chain.ainvoke(body.query)` run in parallel.
   - Combined list: `[rewritten_query] + expanded_queries` (deduped).

5. **Retrieval**  
   - `retrieval_service.search_with_filters(queries=..., org_id, user_id, limit, filter_groups=updated_filters, arango_service=arango_service, knowledge_search=True)`.
   - Returns `{ searchResults, records, status, status_code, ... }`.

6. **Response**  
   - JSONResponse with that payload (and optional `kb_filtering` if KB filter was used).

So for **search**: auth → KB validation (if filters) → query rewrite + expansion → **RetrievalService.search_with_filters** → JSON.

---

## 3. Flow B: POST /api/v1/chat (Search + LLM answer)

**File:** `backend/python/app/api/routes/chatbot.py`

**Steps:**

1. **Auth**  
   Same as above.

2. **Route handler:** `askAI(..., query_info: ChatQuery, retrieval_service, arango_service, reranker_service, config_service)`  
   - Reads query, limit, filters, `previousConversations`, `chatMode`, `quickMode`, etc.

3. **Process chat (search + context building)**  
   - `process_chat_query(...)` (or `process_chat_query_with_status` for streaming) in **same file**:
     - **LLM:** `get_llm_for_chat(config_service, modelKey, modelName, chatMode)` → LLM instance and config.
     - **Follow-up rewriting (if history):**  
       If `previousConversations` non-empty, `setup_followup_query_transformation(llm)` from `query_transform.py` rewrites the current query using conversation history; `query_info.query` is replaced.
     - **Query decomposition (if not quick):**  
       `QueryDecompositionExpansionService` in `app/utils/query_decompose.py` runs `transform_query(query)` → LLM decides decompose_and_expand / expansion / none and returns a list of sub-queries.  
       `all_queries` = that list (or `[query_info.query]` if quick).
     - **Search:**  
       `retrieval_service.search_with_filters(queries=all_queries, org_id, user_id, limit, filter_groups=query_info.filters)` (same core as search route).
     - **Flatten & rerank:**  
       - `get_flattened_results(search_results, blob_store, org_id, is_multimodal_llm, ...)` in `app/utils/chat_helpers.py` turns raw search hits into a flat list of blocks (text, tables, images, etc.), resolving block groups and blob content.
       - If multiple results and not quick mode: `reranker_service.rerank(query, documents=flattened_results, top_k=limit)` in `app/modules/reranker/reranker.py` (cross-encoder) reranks by relevance.
     - **Messages:**  
       System prompt from `get_model_config_for_mode(chatMode)`, then `previousConversations`, then one user message built by `get_message_content(final_results, ..., query_info.query, ..., query_info.mode)` (context + query).
     - **Tools:**  
       `create_fetch_full_record_tool(virtual_record_id_to_result)` from `app/utils/fetch_full_record.py` so the LLM can request full record content.
   - Returns: `llm, messages, tools, tool_runtime_kwargs, final_results, all_queries, virtual_record_id_to_result, blob_store, is_multimodal_llm`.

4. **LLM call with tools**  
   - `resolve_tools_then_answer(llm, messages, tools, tool_runtime_kwargs, max_hops=4)` in **chatbot.py**:
     - Binds tools to LLM (`bind_tools_for_llm` from `app/utils/streaming.py`).
     - `llm_with_tools.ainvoke(messages)`; if the model returns tool calls, the handler runs the tools (e.g. fetch full record), appends `ToolMessage`s, and repeats until no more tool calls or max_hops.
     - Returns final `AIMessage`.

5. **Citations and response**  
   - `process_citations(final_ai_msg, final_results, records=[], from_agent=False)` in `app/utils/citations.py` attaches citation metadata to the answer.
   - JSONResponse with the cited answer.

So for **chat**: auth → LLM + follow-up rewrite → optional query decomposition → **same search_with_filters** → flatten → optional rerank → build messages + tools → **resolve_tools_then_answer** → **process_citations** → JSON.

---

## 4. Flow C: POST /api/v1/agent/.../chat (Agent chat)

**File:** `backend/python/app/api/routes/agent.py`

- Uses the **same** retrieval and reranker services; the difference is **orchestration**.
- **Route:** e.g. `askAI` for `POST /agent-chat` (or agent-specific routes under `/api/v1/agent/`).
- **Flow:**  
  - Get services (logger, arango, reranker, retrieval, config, llm).  
  - Optional LLM response cache (by query + context).  
  - `build_initial_state(...)` packs query, user info, and **references to retrieval_service, arango_service, reranker_service** into the graph state.  
  - `agent_graph.ainvoke(initial_state, config)` runs the LangGraph; **inside the graph**, nodes call retrieval (e.g. same `search_with_filters`) and reranker when the agent decides to search.  
  - Result is taken from `final_state["completion_data"]` (or `response`), optionally cached, then returned (with citations if present).

So **agent** = same retrieval/rerank core, but triggered by graph nodes instead of a single linear pipeline.

---

## 5. Core: RetrievalService.search_with_filters

**File:** `backend/python/app/modules/retrieval/retrieval_service.py`

This is the **single place** where vector search + permissions + result shaping happen for both search and chat (and agent).

**High-level steps:**

1. **Build Arango filters**  
   - `filter_groups` (e.g. `kb`, `departments`, `apps`) is converted to a dict keyed by metadata (e.g. `kb`, `departments`) for Arango.

2. **Parallel init (asyncio.gather)**  
   - **Accessible records:**  
     `_get_accessible_records_task(user_id, org_id, filter_groups, arango_service)`  
     → calls **`arango_service.get_accessible_records(user_id, org_id, filters)`** in `backend/python/app/connectors/services/base_arango_service.py`.  
     That runs an AQL query that unions: user → direct records, user → group → records, user → org → records, user → record groups → records, etc., with app filter (UPLOAD or CONNECTOR with allowed connector IDs). Then applies KB/connector filters. Returns list of record documents (with `_key`, `virtualRecordId`, etc.).
   - **Vector store:**  
     `_get_vector_store_task()`  
     - Ensures collection exists and has points (else raises `VectorDBEmptyError`).  
     - Gets embedding model via `get_embedding_model_instance()` (from config, e.g. BGE).  
     - Builds **QdrantVectorStore** (dense + sparse, hybrid) and caches it on `self.vector_store`.
   - **User (for URLs):**  
     `_get_user_cached(user_id)` for replacing `{user.email}` in mail links, etc.

3. **Permission filter for vector search**  
   - From accessible records, collect `accessible_virtual_record_ids`.  
   - **Vector DB filter:**  
     - If `virtual_record_ids_from_tool`: filter `must={ orgId, virtualRecordId: virtual_record_ids_from_tool }`.  
     - Else: `filter_collection(must={"orgId": org_id}, should={"virtualRecordId": accessible_virtual_record_ids})` (Qdrant filter).  
   Implemented in **`app/services/vector_db/qdrant/qdrant.py`** (`filter_collection`) and used to restrict which points are searched.

4. **Parallel vector searches**  
   - `_execute_parallel_searches(queries, filter, limit, vector_store)`:
     - Embeds all queries in parallel (`dense_embeddings.aembed_query(query)` per query).
     - Builds Qdrant `QueryRequest`s (with `filter`, `limit`, `using="dense"`).
     - **`vector_db_service.query_nearest_points(collection_name, requests)`** in **qdrant.py** runs the actual vector search (batch).
     - Converts points to `(Document, score)` and then `_format_results` → list of `{ score, citationType, metadata, content }`.

5. **Map virtualRecordId → record**  
   - From search result metadata, collect `virtual_record_ids`.  
   - `_create_virtual_to_record_mapping(accessible_records, virtual_record_ids)` builds `virtual_record_id → record` using the already-fetched accessible records (no extra DB call).

6. **Enrich metadata and URLs**  
   - For each search result: set `metadata.recordId`, `origin`, `connector`, `kbId`, `webUrl`, `recordName`, `mimeType`, `extension`, etc. from the record.  
   - If `webUrl` is missing, record is FILE or MAIL: batch-fetch from Arango **files** or **mails** collections and fill `webUrl` / `mimeType`.

7. **Block groups / knowledge_search**  
   - If `knowledge_search` and result has `isBlockGroup`, use `get_record` and blob_store to resolve block content; some results go through `get_flattened_results` and are merged as table/block-group items.

8. **Dedupe, sort, filter**  
   - Sort by score; drop results missing required metadata (`origin`, `recordName`, `recordId`, `mimeType`, `orgId`).

9. **Return**  
   - `{ searchResults, records, status, status_code, message, virtual_to_record_map, appliedFilters? }` or an empty-response dict from `_create_empty_response` (with appropriate status codes: 200/404/503/500).

So **search_with_filters** = Arango (accessible records) + Qdrant (filter + query_nearest_points) + metadata/URL enrichment + optional block flattening.

---

## 5.1 Semantic search vs keyword search (how they’re used)

### What each type is

- **Semantic search** = search by **meaning**. The query and documents are turned into **dense vectors** (embeddings, e.g. from a BGE model). Qdrant finds points whose dense vector is nearest to the query’s dense vector. Good for “find things like this” and paraphrased questions.
- **Keyword search** = search by **lexical overlap** (words/terms). Implemented here as **sparse vectors** (BM25-style) via `FastEmbedSparse(model_name="Qdrant/BM25")`. Good for exact phrases, IDs, and term-heavy queries.

### Where they’re set up

| Layer | File | What happens |
|-------|------|--------------|
| **Indexing** | `app/modules/transformers/vectorstore.py`, `app/modules/indexing/run.py` | Chunks get **both** a dense vector (from the configured embedding model) and a sparse vector (from `FastEmbedSparse` Qdrant/BM25). Stored in Qdrant with `vector_name="dense"` and `sparse_vector_name="sparse"`. Collection is created with both vector types (`type: "keyword"` for sparse). |
| **Query service** | `app/modules/retrieval/retrieval_service.py` | `RetrievalService` initializes **sparse** (`FastEmbedSparse`, Qdrant/BM25) and **dense** (from config, e.g. BGE). When building the vector store for search it uses `RetrievalMode.HYBRID` and passes both `embedding` (dense) and `sparse_embedding`, so the **store** is capable of hybrid search. |

### What actually runs at query time

- **Search execution** is in `retrieval_service._execute_parallel_searches()`:
  1. Only the **dense** embedding of the query is computed: `dense_embeddings.aembed_query(query)`.
  2. A Qdrant `QueryRequest` is built with that dense vector and **`using="dense"`**.
  3. `vector_db_service.query_nearest_points(collection_name, requests)` is called (Qdrant’s batch query API).

So at **query time only semantic (dense) search is used**. The sparse (keyword/BM25) vectors are **indexed** and the vector store is configured for HYBRID, but the query path **never** sends a sparse query vector or uses `using="sparse"` / hybrid in the request. Result: **semantic search = used; keyword search = indexed but not used** in the current query service flow.

### API field `retrievalMode`

- **Chat:** `ChatQuery.retrievalMode` (default `"HYBRID"`) in `api/routes/chatbot.py`.
- **Agent:** Same field in `api/routes/agent.py`; it’s passed into agent state (e.g. `build_initial_state` / chat_query).
- **Retrieval:** `search_with_filters()` does **not** take a retrieval-mode argument. The value of `retrievalMode` is never passed into the retrieval service and does not switch between dense-only and hybrid. So **semantic vs keyword behavior is not changed by this field** today; it’s effectively unused for the actual search.

### Summary

| Aspect | Semantic (dense) | Keyword (sparse / BM25) |
|--------|------------------|--------------------------|
| **Indexing** | Yes (embedding model from config) | Yes (FastEmbedSparse Qdrant/BM25) |
| **Query service search** | Yes – only path used | No – not called at query time |
| **Controlled by** | Embedding model config | N/A at query time |
| **`retrievalMode`** | Not used to select mode | Not used |

To actually use keyword (or hybrid) search in the query service, the code would need to: (1) build a sparse query vector (e.g. same FastEmbedSparse), and (2) call Qdrant with a request that uses sparse or hybrid (e.g. `using` for sparse or a combined strategy) instead of only `using="dense"`.

---

## 5.2 How keyword search actually works (and how it compares to Elasticsearch)

### How it works in this codebase (sparse vectors / BM25-style)

The “keyword” side uses **sparse vectors**, not a separate full-text engine:

1. **Model:** `FastEmbedSparse(model_name="Qdrant/BM25")` (used in indexing and in `RetrievalService`).
2. **Representation:** Text is turned into a **sparse vector**:
   - Vocabulary: each dimension = a term (word/token).
   - For a document (or query): only dimensions for terms that appear are non-zero; the value is a **weight** (term frequency, or TF‑IDF/BM25-style).
   - Example: “Mac and cheese” → something like `{(token_20: 1.0), (token_101: 1.0), (token_501: 1.0)}` (conceptually). Most dimensions are 0.
3. **Storage:** In Qdrant, each point has a **sparse** vector (name `"sparse"`) alongside the dense vector. No separate inverted index in the app — Qdrant stores and indexes these sparse vectors.
4. **Search (if it were used at query time):** Query text → same FastEmbedSparse → sparse query vector. Qdrant would do **similarity search** in sparse space (e.g. dot product or BM25-style scoring over sparse vectors). Documents that share important terms with the query get higher scores — same **idea** as classic keyword/BM25 search.

So “keyword search” here = **BM25-style ranking implemented as sparse vector similarity** inside Qdrant, not a separate Elasticsearch-like server.

### Same idea as Elasticsearch, different implementation

| Aspect | Elasticsearch | This codebase (Qdrant + FastEmbedSparse BM25) |
|--------|----------------|-----------------------------------------------|
| **Goal** | Keyword/lexical search, BM25 ranking | Same: term overlap + IDF-style weighting |
| **Data structure** | **Inverted index**: term → list of (document, positions, freq). | **Sparse vectors**: each doc = vector of (dimension = term, value = weight). |
| **Search** | Look up query terms → merge posting lists → compute BM25 score per doc. | Query → sparse vector → similarity (e.g. dot product) with document sparse vectors → same ranking family as BM25. |
| **Where it runs** | Dedicated search engine (ES cluster). | Inside the same vector DB (Qdrant) that does dense/semantic search. |

So:

- **Same idea:** BM25-style, term frequency, inverse document frequency, keyword matching.
- **Different implementation:** Elasticsearch = inverted index + posting lists; here = sparse vectors + vector similarity. Outcome is similar (documents that share important terms with the query rank higher), but the storage and query path are different. This codebase does **not** use Elasticsearch for search; it uses Qdrant for both dense (semantic) and sparse (keyword-style) vectors, and currently only the dense path is used at query time.

---

## 5.3 What IDs the retrieval service returns (Records, Block groups, Blocks)

**Note:** At query time only **semantic (dense)** search runs; keyword (sparse) is not used. The IDs below come from that single search path.

The retrieval service returns two main structures:

### 1. `searchResults` (list of hits)

Each element is a hit with **score**, **content** (text from the vector point), and **metadata**. The metadata includes IDs that identify the **record**, and the **block or block group** the hit came from:

| Field | Source | Meaning |
|-------|--------|--------|
| **recordId** | Added by retrieval (from Arango) | Arango record `_key` — the **Record** this hit belongs to. |
| **virtualRecordId** | From vector DB payload | Logical document ID; one record can have one virtualRecordId, used to group blocks. |
| **blockIndex** | From vector DB payload | Index of the **Block** (or block group) within that record. |
| **isBlockGroup** | From vector DB payload | `true` = hit is a **block group** (e.g. table); `false` = single block. |
| **point_id** | Added by retrieval (Qdrant point id) | ID of the vector point in Qdrant. |

So from each hit you get:

- **Record:** `recordId` (and full record in `records`).
- **Block or block group:** `virtualRecordId` + `blockIndex` + `isBlockGroup` (and optionally `point_id`).

Required metadata for a hit to be returned: `origin`, `recordName`, `recordId`, `mimeType`, `orgId` (all set by retrieval from accessible records).

### 2. `records` (list of full record documents)

List of **full Record documents** from Arango (RECORDS collection) for every distinct `recordId` that appeared in the hits. So you get both:

- Per-hit IDs (record + block/block group) in **searchResults[].metadata**.
- Full record objects in **records**.

### Summary

| You want | Where to get it |
|----------|------------------|
| **Record ID** | `searchResults[i].metadata.recordId`; full record in `records` (match by `_key`). |
| **Block** | `searchResults[i].metadata.virtualRecordId` + `blockIndex` with `isBlockGroup === false`. |
| **Block group** | Same metadata with `isBlockGroup === true`. |
| **Vector point** | `searchResults[i].metadata.point_id`. |

All of this is returned after the **same** search (currently semantic-only); there is no separate “keyword search” result set — keyword is indexed but not used at query time.

---

## 5.4 What block index and block group index are used for

A record’s content is stored as **blocks** (paragraphs, images, table rows, etc.) and **block groups** (e.g. a table = one block group containing many TABLE_ROW blocks). The record has:

- `block_containers.blocks` — list of individual blocks; each has an `index` (0-based position).
- `block_containers.block_groups` — list of block groups; each has an index (0-based position).

**blockIndex** in search result metadata means:

- **isBlockGroup === false:** index into **blocks[]**. So `blocks[blockIndex]` is the exact block (paragraph, image, table row, etc.) that was embedded and matched.
- **isBlockGroup === true:** index into **block_groups[]**. So `block_groups[blockIndex]` is the block group (e.g. a table) that was embedded and matched.

**Uses of block index / block group index:**

| Use | How |
|-----|-----|
| **Locate content** | Given a hit (virtualRecordId, blockIndex, isBlockGroup), load the record and do `blocks[blockIndex]` or `block_groups[blockIndex]` to get the actual block/block group content (text, table, etc.) for display and for the LLM context. |
| **Citations** | Block numbers shown to the user are like `R1-5` (record 1, block 5). So block_index is used to form the human-readable citation (e.g. in `get_message_content` in chat_helpers.py) so the user knows which part of the document the answer came from. |
| **Deduplication** | Chunk IDs are built as `virtual_record_id` + `block_index` (and `-block_group` for block groups). That avoids including the same block or block group twice when merging/ranking search results. |

So **block index** = “which position in blocks[] or block_groups[] this hit refers to” — the lookup key to get the exact content and to build citations.

---

## 6. Supporting pieces (file ↔ role)

| What | File | Role |
|------|------|------|
| Query rewrite + expansion (search) | `app/utils/query_transform.py` | `setup_query_transformation(llm)` → rewrite chain + expansion chain (LLM). |
| Follow-up rewrite (chat) | Same | `setup_followup_query_transformation(llm)` for conversation-aware query. |
| Query decomposition (chat) | `app/utils/query_decompose.py` | `QueryDecompositionExpansionService.transform_query` → decompose_and_expand / expansion / none + sub-queries. |
| Flatten search results to blocks | `app/utils/chat_helpers.py` | `get_flattened_results`, `get_message_content`, `get_record` (block groups, tables, images). |
| Reranker | `app/modules/reranker/reranker.py` | Cross-encoder rerank of flattened docs; used in chat (and agent) when not quick mode. |
| Accessible records | `app/connectors/services/base_arango_service.py` | `get_accessible_records(user_id, org_id, filters)` – AQL over users, groups, orgs, permissions, record groups. |
| Vector DB (Qdrant) | `app/services/vector_db/qdrant/qdrant.py` | `filter_collection`, `query_nearest_points`; used by retrieval_service. |
| Vector store (LangChain) | `app/modules/retrieval/retrieval_service.py` | `_get_vector_store_task()` builds `QdrantVectorStore` (dense + sparse, hybrid). |
| Config / embedding / LLM | `app/config/configuration_service.py`, `app/utils/aimodels.py` | Embedding model name and instance; LLM for routes and decomposition. |
| Container (query app) | `app/containers/query.py`, `app/containers/utils/utils.py` | `QueryAppContainer`; `create_retrieval_service`, `get_vector_db_service`, `create_arango_service`, etc. |

---

## 7. End-to-end “when I query” summary

- **Request** hits Node → proxied to Python query service (`/api/v1/search` or `/api/v1/chat` or agent).
- **Auth** in `query_main.py` middleware; `userId` and `orgId` on `request.state.user`.
- **Search path:**  
  Optional KB validation → **query_transform** (rewrite + expansion) → **RetrievalService.search_with_filters** → JSON.
- **Chat path:**  
  LLM + optional follow-up rewrite → optional **query_decompose** → **same search_with_filters** → **chat_helpers.get_flattened_results** → optional **reranker** → build messages + **fetch_full_record** tool → **resolve_tools_then_answer** → **process_citations** → JSON.
- **Inside search_with_filters:**  
  **Arango** `get_accessible_records` → **Qdrant** `filter_collection` + `query_nearest_points` → enrich with record metadata and URLs → optional block flattening → return searchResults + records.

All “query” flows that need documents go through **RetrievalService.search_with_filters**; the rest is routing, query shaping, reranking, and LLM/tool/citation handling in the route and helpers above.

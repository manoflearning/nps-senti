import json
line = open('data_crawl/forum_ppomppu.jsonl', encoding='utf-8').readline()
obj = json.loads(line)
text = obj.get('text', '')
print('Text length:', len(text))
print('Text content (first 500 chars):')
print(repr(text[:500]))
print()
print('=== Comments from extra.forum.comments ===')
if 'extra' in obj and 'forum' in obj['extra']:
    for i, c in enumerate(obj['extra']['forum']['comments'][:2]):
        print(f'Comment {i}: {repr(c.get("text", "")[:80])}')

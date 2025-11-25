import json
from preprocess.preprocess_ppomppu.stage2_transform import remove_comments_from_text
from preprocess.preprocess_ppomppu.stage1_models_io import RawPpomppuComment

line = open('data_crawl/forum_ppomppu.jsonl', encoding='utf-8').readline()
obj = json.loads(line)
text = obj.get('text', '')

# 댓글 만들기
comments_raw = obj['extra']['forum']['comments']
comments = [RawPpomppuComment(
    id=c.get('id'),
    text=c.get('text', ''),
    published_at=c.get('publishedAt'),
    author=c.get('author'),
    depth=c.get('depth')
) for c in comments_raw]

print('First 3 comments:')
for i, c in enumerate(comments[:3]):
    print(f'{i}: {repr(c.text[:50])}')

print()
print('Lines that match comments:')
lines = text.split('\n')
comment_texts_raw = [c.text.strip() for c in comments]
for i, line in enumerate(lines):
    line_stripped = line.strip()
    if any(line_stripped == comment for comment in comment_texts_raw):
        print(f'Line {i}: MATCHES - {repr(line_stripped[:60])}')

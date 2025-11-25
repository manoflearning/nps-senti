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

print('Original text length:', len(text))
print('Comments count:', len(comments))
print('First comment:', repr(comments[0].text[:80]))

result = remove_comments_from_text(text, comments)
print()
print('Result text length:', len(result))
print('Result text (first 400 chars):')
print(repr(result[:400]))

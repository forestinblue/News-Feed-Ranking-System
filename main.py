from database import query, database
import time
import pandas as pd

# 최대 리뷰 좋아요 수 구하기.
def getMaxReviewLike():
    sql = "SELECT COUNT(ReviewLikes.reviewId) FROM Reviews LEFT JOIN ReviewLikes ON ReviewLikes.reviewId = Reviews.id GROUP BY Reviews.id ORDER BY COUNT(ReviewLikes.reviewId) DESC"
    maxReviewLike = query(sql)
    return maxReviewLike[0]['COUNT(ReviewLikes.reviewId)']


# 최대 리뷰 댓글 수 구하기.
def getMaxReviewComment():
    sql = "SELECT COUNT(ReviewComments.reviewId) FROM Reviews LEFT JOIN ReviewComments ON ReviewComments.reviewId = Reviews.id GROUP BY Reviews.id ORDER BY COUNT(ReviewComments.reviewId) DESC"
    maxReviewComment = query(sql)
    return maxReviewComment[0]['COUNT(ReviewComments.reviewId)']


# 최대 유저 팔로잉 수 구하기.
def getMaxFollow():
    sql = "SELECT COUNT(UserFollows.followingId) FROM Users LEFT JOIN UserFollows ON UserFollows.followingId = Users.id GROUP BY Users.id ORDER BY COUNT(UserFollows.followingId) DESC"
    getMaxFollow = query(sql)
    return getMaxFollow[0]['COUNT(UserFollows.followingId)']


# 리뷰의 좋아요 점수 구하기.
def getScoreOfReviewLike(reviewId):
    global maxReviewScore
    sql = "SELECT COUNT(reviewId) FROM ReviewLikes WHERE reviewId = {0}".format(reviewId)
    score = pd.read_sql_query(sql, database)
    return score['COUNT(reviewId)'][0] / maxReviewScore * 10

# 리뷰의 댓글 점수 구하기.
def getScoreOfReviewComment(reviewId):
    global maxCommentScore
    sql = "SELECT COUNT(id) FROM ReviewComments WHERE reviewId = {0}".format(reviewId)
    score = pd.read_sql_query(sql, database)
    return score['COUNT(id)'][0] / maxCommentScore * 10

# 리뷰의 팔로우 점수 구하기.
def getScoreOfFollow(reviewId):
    global maxFollowScore
    sql = "SELECT COUNT(UserFollows.followerId) FROM UserFollows LEFT JOIN Reviews ON Reviews.authorId = UserFollows.followingId WHERE Reviews.id = {0}".format(reviewId)
    score = pd.read_sql_query(sql, database)
    return score['COUNT(UserFollows.followerId)'][0] / maxFollowScore * 10

# 대중성 스코어 구하기
def getPopularityScore(reviewId):
    return getScoreOfReviewLike(reviewId) + getScoreOfReviewComment(reviewId) + getScoreOfFollow(reviewId)

# 최신성 + 대중성 점수
def getTotalScore(reviewId):
    return getRecencyScore(reviewId) + getPopularityScore(reviewId)

# 친밀성 점수 구하기
def getTasteScore(userId, reviewId):
    score = 0
    sql = 'SELECT id FROM Reviews LEFT JOIN UserFollows ON UserFollows.followingId = Reviews.authorId WHERE Reviews.id = {0} and UserFollows.followerId = {1}'.format(reviewId, userId)
    checkFollows = pd.read_sql_query(sql, database)
    if len(checkFollows) > 0:
        score += 5

    sql = 'SELECT * FROM BookmarkReviews ' \
          'LEFT JOIN Reviews as authorReview ON BookmarkReviews.reviewId = authorReview.id ' \
          'LEFT JOIN Reviews as seeingReview ON seeingReview.authorId = authorReview.authorId ' \
          'LEFT JOIN Bookmarks ON BookmarkReviews.bookmarkId = Bookmarks.id ' \
          'WHERE Bookmarks.userId = {0} and seeingReview.id = {1}'.format(userId, reviewId)
    checkBookmarks = pd.read_sql_query(sql, database)
    if len(checkBookmarks) > 0:
        score += 5

    sql = 'SELECT * FROM ReviewComments ' \
          'LEFT JOIN Reviews as authorReview ON ReviewComments.reviewId = authorReview.id ' \
          'LEFT JOIN Reviews as seeingReview ON seeingReview.authorId = authorReview.authorId ' \
          'WHERE ReviewComments.authorId = {0} and seeingReview.id = {1}'.format(userId, reviewId)
    checkComments = pd.read_sql_query(sql, database)
    if len(checkComments) > 0:
        score += 5

    sql = 'SELECT * FROM ReviewLikes ' \
          'LEFT JOIN Reviews as authorReview ON ReviewLikes.reviewId = authorReview.id ' \
          'LEFT JOIN Reviews as seeingReview ON seeingReview.authorId = authorReview.authorId ' \
          'WHERE ReviewLikes.senderId = {0} and seeingReview.id = {1}'.format(userId, reviewId)
    checkLikes = pd.read_sql_query(sql, database)
    if len(checkLikes) > 0:
        score += 5

    return score

# 메인 함수.
if __name__ == '__main__':

    firstReviewTime = query('SELECT createdAt FROM Reviews ORDER BY createdAt ASC LIMIT 1')[0]['createdAt']
    lastReviewTime = query('SELECT createdAt FROM Reviews ORDER BY createdAt DESC LIMIT 1')[0]['createdAt']

    maxReviewScore = getMaxReviewLike()
    maxCommentScore = getMaxReviewComment()
    maxFollowScore = getMaxFollow()

    Reviews = pd.read_sql_query('SELECT * FROM Reviews WHERE NOT authorId = 928', database)
    # 300개 랜덤 추출.
    Reduced_Reviews = Reviews.sample(n=300)
    # 최신성 적용.
    Reduced_Reviews['score'] = (Reduced_Reviews['createdAt'] - firstReviewTime) / (lastReviewTime - firstReviewTime) * 50
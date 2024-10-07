# импорт всех необходимых библиотек
import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from typing import List
from loguru import logger
from sqlalchemy import create_engine
from schema import PostGet
from datetime import datetime

from catboost import CatBoostClassifier

app = FastAPI()


SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"

engine = create_engine(SQLALCHEMY_DATABASE_URL)

def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models():
    model_path = get_model_path("/my/super/path")
    model = CatBoostClassifier()
    model.load_model(model_path)
    return model

def load_features() -> pd.DataFrame:
    query_user_df = 'SELECT * FROM mai_user_featurs_final'
    user_df = batch_load_sql(query_user_df)
    user_df = user_df.drop(['index'], axis=1)
    user_df.set_index('Unnamed: 0', inplace=True)
    user_df.index.name = None
    return user_df

def load_post_features() -> pd.DataFrame:
    query_post_df = 'SELECT * FROM mai_post_featurs_final'
    post_f = batch_load_sql(query_post_df)
    post_f = post_f.drop(['index'], axis=1)
    post_f.set_index('Unnamed: 0', inplace=True)
    post_f.index.name = None
    return post_f


logger.info("Start")

#загружаем модель
logger.info("load_models")
model = load_models()

#загружаем таблицу с фичами для всех юзеров
logger.info("load_user_features")
user_ftch = load_features()

#загружаем таблицу с фичами текстами постов

logger.info("load_post_features")
post_ftch = load_post_features()

logger.info("loads over")


@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(id: int, time: datetime, limit: int = 5) -> List[PostGet]:

    # формируем дф для предсказаний, на основе фичей всех постов и нашего юзера
    df_to_ml = post_ftch.merge(user_ftch[user_ftch['user_id']==id], how='cross')


    # добавляем текущую дату как признак
    df_to_ml['month'] = time.month
    df_to_ml['day'] = time.day

    # Удаляем юзер_ид и переводит post_id в индекс для возможности напрямую связать предсказания с постами
    df_to_ml = df_to_ml.drop(['user_id', 'text'], axis=1).set_index('post_id')

    # предсказываем и сохраняем вероятности для каждого поста
    predict_prob = model.predict_proba(df_to_ml)[:, 1]
    df_to_ml['predict_prob'] = predict_prob

    # оставляем id лучших постов
    posts_by_prob = df_to_ml.sort_values('predict_prob', ascending=False).head(limit).index

    # Приводим вывод к нужному формату
    recommendation = []
    for ids in posts_by_prob:
        final_list = post_ftch.loc[post_ftch['post_id'] == ids, ['text', 'topic']].iloc[0]
        post = {
            "id": ids,
            "text": final_list['text'],
            "topic": final_list['topic']}
        recommendation.append(post)

    return recommendation


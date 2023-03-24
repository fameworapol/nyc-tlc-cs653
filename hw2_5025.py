import boto3
import pandas as pd
import numpy as np

if __name__ == "__main__":
    dict_df = {
        '2017-01': None,
        'all': None
    }

    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    bucket = 'nyc-tlc-cs653-5035'

    bucket_resource = s3_resource.Bucket(bucket)

    frames = []

    for key in bucket_resource.objects.all():

        filename = key.key
        print(filename)

        if filename.find('yellow_tripdata_2017') != -1:

            # generate_presigned_url โดยกำหนด params data_source โดยระบุชื่อ Bucket และ Key(ชื่อไฟล์) ของเราบน S3
            data_source = {
                'Bucket': bucket,
                'Key': filename
            }
            url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params=data_source
            )

            # ใช้ pandas อ่านไฟล์ตามที่เรา generate_presigned_url ไปด้านบน ใช้ pyarrow เป็น engine ในการอ่านไฟล์
            df = pd.read_parquet(url, engine='pyarrow')

            # ถ้าชื่อไฟล์มี 2017-01 อยู่ จะ assign ให้อยู่ใน Dictionaries ที่มี key เป็น '2017-01'
            if filename.find('2017-01') != -1:
                dict_df['2017-01'] = df

            # เก็บ Dataframe เข้า list frames
            frames.append(df)

    # รวม Dataframe เดือน มกราคม - มีนาคม 2017 เข้าด้วยกัน โดยไม่สนใจ index
    dict_df['all'] = pd.concat(frames, ignore_index=True)

    # ลบ List frames ทิ้ง เพื่อประหยัด Memory
    del frames

    # ให้ตั้ง Index ใหม่ เพื่อจะเอา Column index มาใช้
    dict_df['2017-01'].reset_index(inplace=True)

    # ในเดือน Jan 2017 มีจำนวน yellow taxi rides ทั้งหมดเท่าไร แยกจำนวน rides ตามประเภทการจ่ายเงิน (payment)
    # ANSWER: GROUP BY column 'patment_type' แล้วก็นับจำนวน index ที่มีในแต่ละ payment_type ก็จะได้คำตอบ
    a = dict_df['2017-01'].groupby(['payment_type']).index.count()
    print(a)

    # ในเดือน Jan 2017 Yellow taxi rides ในแต่ละจุดรับผู้โดยสาร (Pickup location) เป็นจำนวน rides มากน้อยเท่าไร
    # และมีค่าโดยสารรวมของ rides และจำนวนผู้โดยสารเฉลี่ยต่อ rides ในแต่ละจุดเท่าไร

    # ANSWER: GROUP BY column 'PULocationID' แล้วก็สร้าง List ของ 'fare_amount' ขึ้นมาแยกตาม 'PULocationID'
    # จากนั้นก็นับจำนวนใน List ที่สร้างขึ้นมาแล้วก็ตั้ง Column 'TotalRides' ขึ้นมา
    # หลังจากนั้นก็ Summary List เพื่อให้ได้ค่าโดยสารทั้งหมดใน 'fare_amount' ของ List และสร้าง Column 'TotalFare'
    # และคำนวณค่าเฉลี่ยของ List 'fare_amount' เอาไปใส่ไว้ใน Column 'MeanFare'
    b = (dict_df['2017-01'].groupby('PULocationID')['fare_amount']
         .apply(lambda x: list(x.unique()))
         .reset_index()
         .assign(TotalRides=lambda d: d['fare_amount'].str.len(),
                 TotalFare=lambda d: d['fare_amount'].apply(np.sum),
                 MeanFare=lambda d: d['fare_amount'].apply(np.mean))
         )
    b.drop(['fare_amount'], axis=1, inplace=True)
    print(b)
    del dict_df['2017-01']

    # ในเดือน Jan - Mar 2017 มีจำนวน yellow taxi rides ทั้งหมดเท่าไร แยกจำนวน rides ตามประเภทการจ่ายเงิน (payment)
    dict_df['all'].reset_index(inplace=True)
    c = dict_df['all'].groupby(['payment_type']).index.count()
    print(c)
/*
## DFS Meta ##

1.表设计
文件元数据表，file_metadata
+-------------+---------------+------+-----+-------------------+-----------------------------+
| Field       | Type          | Null | Key | Default           | Extra                       |
+-------------+---------------+------+-----+-------------------+-----------------------------+
| id          | varchar(50)   | NO   | PRI | NULL              |                             |
| name        | varchar(255)  | YES  |     | NULL              |                             |
| biz         | varchar(50)   | NO   |     | NULL              |                             |
| md5         | varchar(50)   | NO   |     | NULL              |                             |
| user_id     | varchar(30)   | NO   |     | NULL              |                             |
| domain      | bigint        | NO   | MUL | NULL              |                             |
| size        | int(11)       | NO   |     | NULL              |                             |
| chun_size   | int(11)       | NO   |     | NULL              |                             |
| upload_time | timestamp     | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
| entity_type | tinyint(4)    | NO   |     | NULL              |                             |
| ref_cnt     | int(11)       | NO   |     | 1                 |                             |
+-------------+---------------+------+-----+-------------------+-----------------------------+

说明：
id : fileId , 长度为50个数字字母的组合，如“597edb8e4ec50300d28915d4”
name ： 文件名称，可为空，最大可以存储60个字母，数字或汉字，实际占用存储空间会按照字符串的大小计算
biz ：模块名称，最大长度50
md5 : md5字符串，长度为50个数字字母的组合，如“5e90fb4a95b33de2bfadd2ed8b4aa357”
user_id : 用户id，最大长度为30
domian ：公司id，bigint,最大值为2的64次方
size ： 文件大小，单位字节，最大可以存储4096M文件大小
chun_size : 文件块儿大小，单位字节
upload_time : 上传时间，格式yyyy-mm-dd HH:MM:SS
entity_type : 文件存储类型，每种类型用一个整数表示，最大值为255
ref_cnt : 使用次数，默认值是1,表示文件本身；当文件被一次引用时，值加1
对（domian，md5）建立复合索引



引用文件数据表，duplicate_info
+-------------+---------------+------+-----+-------------------+-----------------------------+
| Field       | Type          | Null | Key | Default           | Extra                       |
+-------------+---------------+------+-----+-------------------+-----------------------------+
| dId         | varchar(50)   | NO   | PRI | NULL              |                             |
| fId         | varchar(50)   | NO   | FOR | NULL              |                             |
| created_time| timestamp     | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
+-------------+---------------+------+-----+-------------------+-----------------------------+
说明：
dId :  长度为50个下划线数字字母的组合，如“_59828e534ec50300d2891b33”
fId : 外键，表file_metadata的主键，一个fId可以对应多个dId
created_time : 操作时间，格式yyyy-mm-dd HH:MM:SS


扩展属性表，ext_attr
+-------------+---------------+------+-----+-------------------+-----------------------------+
| Field       | Type          | Null | Key | Default           | Extra                       |
+-------------+---------------+------+-----+-------------------+-----------------------------+
| id          | int(11)       | NO   | PRI | NULL              |                             |
| fId         |  varchar(50)  | NO   | FOR | NULL              |                             |
| attr        | varchar(255) | YES  |     | NULL              |                             |
+-------------+---------------+------+-----+-------------------+-----------------------------+
说明：
id : int类型，最大值4294967296
fId : 外键，表file_metadata的主键，一个fId可以对应多个id
attr ：varchar(255)

sql:
create database file character set = utf8;

create table file_metadata (id varchar(50) primary key,name varchar(255),biz varchar(50) not null,
md5 varchar(50) not null, user_id varchar(30) not null, domain bigint unsigned not null,size int unsigned not null,
chun_size int unsigned not null,ref_cnt int unsigned not null,upload_time timestamp,entity_type tinyint not null,index (domain,md5)) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table duplicate_info(dId varchar(50) primary key,fId varchar(50) not null,created_time timestamp,constraint fk_df_id foreign key(fId) references file_metadata(id) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table ext_attr (id int unsigned auto_increment primary key,fId varchar(50) not null,attr varchar(255) ,constraint fk_ef_id foreign key(fId) references file_metadata(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8;

2.业务接口设计
（1）保存文件元数据
func Save(*FileMetadata) error
将数据存储在表file_metadata中

（2）存储复制数据
func DuplicateWithId(fid string, did string, createDate time.Time)(string, error)
这里的fid可以是实体文件ID（eid），也可以是复制的ID（did），
如果是复制的ID，则需要根据复制的ID找到元数据fid，将其存入表记录。
如果did为空，则生成一个did，id生成策略:使用mongodb的bson.ObjectId，和现有的一致；
将复制数据保存在表duplicate_info中，同时根据fId更新表file_metadata字段used_times值，加1；
返回did


（3）根据fid获取文件元数据对象
func Find(fid string)(*FileMetadata, error)
如果fid以下划线“_"开头则查询表表duplicate_info，file_metadata获取元数据内容；
否则查询表file_metadata获取元数据内容


（4）func FindByMD5(md5 string, domain int64)(*FileMetadata, error)
根据domain，md5获得所有元数据对象，并按上传时间排序，获取最新时间对象返回


（5）func Delete(fid string)(bool, entityIdToBeDeleted string, error)
如果fid不带下划线，在表file_metadata中查找记录，如果used_times>=2,对字段used_times值进行减一操作更新记录并返回false和空串，
否则删除此条记录，并返回true和fid值（非引用id）；
如果fid带下划线，在表duplicate_info和表file_metadata中查找记录，如果used_times>=2,
对表file_metadata字段used_times值进行减一操作更新记录并返回false和空串，否则删除表duplicate_info和表file_metadata中记录，
返回true和fid值（引用id）。


3. 数据操作结构设计
（1）元数据操作
type FileMetadataDO struct {
	FId, Name, Biz, Md5, UserId, ExtAttr string
	Domain, Size                         int64
	ChunkSize                            int
	UploadTime                           time.Time
	EntityType                           int
}

type FileMetadataDAO interface {
	AddFileMeta(fm *FileMetadataDO) (int, error)                                    //表file_metadata添加记录
	DeleteFileMeta(fId string) (int, error)                                         // 删除记录
	UpdateRileMetaRefCnt(fId string, refCount int) (int, error)                     //更新记录字段ref_cnt
	FindFileMeta(fId string) (*FileMetadataDO, error)                               //查询记录
	FindFileMetaWithExtAttr(fId string) (*FileMetadataDO, error)                    //查询表file_metadata和ext_attr
	FindFileMetaByMD5WithExtAttr(md5 string, domain int64) (*FileMetadataDO, error) //查询表file_metadata和ext_attr

	AddExtAttr(fId string, extAttr string) (int, error)                         //在表ext_attr插入一条数据
	DeleteExtAttr(fId string) (int, error)                                      //根据fId删除所有扩展属性
	FindExtAttrByFId(fId string) (string, error)                                //根据fId查询所有扩展属性
	AddDuplicateInfo(fId string, dId string, createDate time.Time) (int, error) //在表duplicate_info插入一条数据
	DeleteDupInfoByDId(dId string) (int, error)                                 //根据dId删除复制数据
	DeleteDupInfoByFid(fId string) (int, error)                                 //根据fId删除所有复制数据
	FindDupInfoByDId(dId string) (string, time.Time, error)                     //根据dId查询数据
}


*/

package sql

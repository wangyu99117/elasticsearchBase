package com.wangyu.test;

import com.wangyu.test.util.ReflectionUtils;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.util.CollectionUtils;
import net.sf.cglib.beans.BeanMap;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by wangyu21 on 2017/8/8.
 */
public class EsBaseDao<T, PK extends Serializable> implements CheckData<T>{
    protected Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Resource
    protected ElasticsearchTemplate esTemplateWrite;

    @Resource
    protected ElasticsearchTemplate esTemplateRead;
    protected Class<T> entityClass;
    private Field identifierProperty;//主键
    private Field routingProperty;  //路由字段
    private ElasticsearchPersistentEntity esPersistentEntity;
    private Client writeClient;    //写节点
    private Client readClient;     //读节点

    @PostConstruct
    public void init() {
        this.entityClass = ReflectionUtils.getSuperClassGenricType(getClass());
        Field[] fields = this.entityClass.getDeclaredFields();
        try{
            esPersistentEntity = esTemplateWrite.getPersistentEntityFor(entityClass);
            writeClient = esTemplateWrite.getClient();
            readClient = esTemplateRead.getClient();
            for(Field field : fields){
                if(field.isAnnotationPresent(org.springframework.data.annotation.Id.class)){
                    identifierProperty = field;
                    identifierProperty.setAccessible(true);
                }
                if(field.isAnnotationPresent(Route.class)){
                    routingProperty = field;
                    routingProperty.setAccessible(true);
                }
            }
            LOGGER.info("-------------------Elasticsearch operate class " + entityClass + " init success!-----------------------------");
        } catch (Exception e) {
            LOGGER.error("-------------------Elasticsearch operate class init fail!----------------------------- class Name is " + entityClass, e);
        }
    }

    public boolean save(T obj){
        checkData(obj);
        try{
            IndexRequestBuilder res = writeClient.prepareIndex()
                    .setIndex(esPersistentEntity.getIndexName())
                    .setType(esPersistentEntity.getIndexType())
                    .setId(identifierProperty.get(obj).toString())
                    .setRouting(routingProperty.get(obj).toString())
                    .setSource((obj));

            res.execute().actionGet();
            return true;
        } catch (IllegalAccessException e) {
            LOGGER.error("-------------------Elasticsearch save obj error!----------------------------- the obj's class Name is " + entityClass, e);
        } catch (Exception e) {
            LOGGER.error("-------------------Elasticsearch save obj error!----------------------------- the obj's class Name is " + entityClass, e);
        }
        return false;
    }


    public boolean refresh() {
        try{

            esTemplateWrite.refresh(entityClass);

            return true;
        }catch (Exception e ){
            LOGGER.error("Elasticsearch index " + entityClass.getSimpleName() + " 's refresh function error is ",e);
            return false;
        }
    }

    public boolean save(List<T> list){
        if(CollectionUtils.isEmpty(list)){
            throw new NullPointerException("传入的list为空");
        }

        try{
            BulkRequestBuilder bulkRequestBuilder = writeClient.prepareBulk();
            for (T obj : list) {
                checkData(obj);
                IndexRequestBuilder res = writeClient.prepareIndex()
                        .setIndex(esPersistentEntity.getIndexName())
                        .setType(esPersistentEntity.getIndexType())
                        .setId(identifierProperty.get(obj).toString())
                        .setRouting(routingProperty.get(obj).toString())
                        .setSource(BeanMap.create(obj));
                bulkRequestBuilder.add(res).request();
            }
            bulkRequestBuilder.execute();
            return true;
        } catch (Exception e){
            LOGGER.error("-------------------Elasticsearch batch save error!---------class Name is " + entityClass + ", batchNum = {} ", list.size(), e);
            return false;
        }
    }

    /**
     * 默认空实现
     * @param obj
     */
    @Override
    public void checkData(T obj) {
        return ;
    }

    public T getById(PK id){
        try{
            T obj = null;
            SearchResponse searchResponse = readClient.prepareSearch(esPersistentEntity.getIndexName())
                    .setTypes(esPersistentEntity.getIndexType())
                    .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(identifierProperty.getName(), id)))
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .execute().actionGet();
            SearchHits searchHits = searchResponse.getHits();
            if(null != searchHits && searchHits.getHits().length > 0) {
                obj = JSON.parseObject(searchHits.getHits()[0].getSourceAsString(), entityClass);
            }
            return obj;
        } catch (Exception e){
            LOGGER.error("-------------------Elasticsearch find obj by id = {} error!----------------------------- the obj's class Name is " + entityClass, id, e);
            return null;
        }
    }

    public Client getReadClient() {
        return readClient;
    }

    public Client getWriteClient() {
        return writeClient;
    }

    public ElasticsearchPersistentEntity getEsPersistentEntity() {
        return esPersistentEntity;
    }
}

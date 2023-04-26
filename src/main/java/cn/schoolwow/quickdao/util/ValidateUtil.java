package cn.schoolwow.quickdao.util;

import cn.schoolwow.quickdao.domain.internal.ManipulationOption;

public class ValidateUtil {
    /**
     * 检查是否设置了唯一字段
     * */
    public static void checkUniqueFieldNames(ManipulationOption option){
        if(option.uniqueFieldNames.isEmpty()){
            throw new IllegalArgumentException("请先调用uniqueFieldNames方法指定唯一性约束字段!");
        }
    }
}

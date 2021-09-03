package com.yangtzelsl.tmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 订单事件数据实体类
 *
 * 34729,create,,1558430842
 * 34730,create,,1558430843
 * 34729,pay,sd76f87d6,1558430844
 * 34730,pay,3hu3k2432,1558430845
 * 34731,create,,1558430846
 * 34731,pay,35jue34we,1558430849
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    private Long userId;
    private String action;
    private String orId;
    private Long timestamp;
}

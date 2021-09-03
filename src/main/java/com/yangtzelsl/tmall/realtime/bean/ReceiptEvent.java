package com.yangtzelsl.tmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 到账事件数据实体类
 *
 * ewr342as4,wechat,1558430845
 * sd76f87d6,wechat,1558430847
 * 3hu3k2432,alipay,1558430848
 * 8fdsfae83,alipay,1558430850
 * 32h3h4b4t,wechat,1558430852
 * 766lk5nk4,wechat,1558430855
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReceiptEvent {
    private String orId;
    private String payEquipment;
    private Long timestamp;
}

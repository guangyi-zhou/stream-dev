package com.gy.disc.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.gy.disc.bean.TableUserInfoBean
 * @Author guangyi_zhou
 * @Date 2025/5/12 21:05
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableUserInfoBean {
    public String uid;
    public String uname;
    public String gender;
    public String user_leven;
    public String login_name;
    public String email;
    public String birthday;
    public Long ts_ms;
}

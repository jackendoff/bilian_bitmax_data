import smtplib
from email.header import Header
from email.mime.text import MIMEText

# 第三方 SMTP 服务


#
# mail_host = "smtp.163.com"  # SMTP服务器
# mail_user = "jackendoff"  # 用户名
# mail_pass = "pf1783126868"  # 授权密码，非登录密码
#
# sender = 'jackendoff@163.com'# 发件人邮箱(最好写全, 不然会失败)
# receivers = ['1783126868@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
#
# # content = content
# title = '人生苦短'  # 邮件主题


def sendEmail(content,title):
    mail_host = "smtp.163.com"  # SMTP服务器
    mail_user = "jackendoff"  # 用户名
    print('邮箱密码没有输入，请在custom-send_email中line24输入')
    mail_pass = "pf***********"  # 授权密码，非登录密码

    sender = 'jackendoff@163.com'  # 发件人邮箱(最好写全, 不然会失败)
    receivers = ['1783126868@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    # content = content
    # title = 'jackendoff'

    message = MIMEText(content, 'plain', 'utf-8')  # 内容, 格式, 编码
    message['From'] = "{}".format(sender)
    message['To'] = ",".join(receivers)
    message['Subject'] = title

    try:
        smtpObj = smtplib.SMTP_SSL(mail_host, 465)  # 启用SSL发信, 端口一般是465
        smtpObj.login(mail_user, mail_pass)  # 登录验证
        smtpObj.sendmail(sender, receivers, message.as_string())  # 发送
        print("mail has been send successfully.")
    except smtplib.SMTPException as e:
        print(e)


def send_email2(SMTP_host, from_account, from_passwd, to_account, subject, content):
    email_client = smtplib.SMTP(SMTP_host)
    email_client.login(from_account, from_passwd)
    # create msg
    msg = MIMEText(content, 'plain', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')  # subject
    msg['From'] = from_account
    msg['To'] = to_account
    email_client.sendmail(from_account, to_account, msg.as_string())

    email_client.quit()

# if __name__ == '__main__':
    # sendEmail()
    # receiver = '***'
    # send_email2(mail_host, mail_user, mail_pass, receiver, title, content)

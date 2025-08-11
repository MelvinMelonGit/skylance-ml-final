#!/usr/bin/env python
# coding: utf-8

# In[1]:


# helper for local development
import os
os.environ.setdefault("DB_URL", "mysql+pymysql://skylanceuser:strongpassword@146.190.97.82:3306/skylance")

# pull it back out into a module-level variable
DB_URL = os.environ["DB_URL"]


# In[ ]:





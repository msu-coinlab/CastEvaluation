adjacency['Garrett'] = ['Allegany']
adjacency['Allegany'] = ['Garrett', 'Washington']
adjacency['Washington'] = ['Allegany', 'Frederick']
adjacency['Frederick'] = ['Washington', 'Carroll', 'Howard', 'Montgomery']
adjacency['Carroll'] = ['Frederick', 'Baltimore', 'Howard']
adjacency['Baltimore'] = ['Carroll', 'Harford', 'Baltimore City', 'Howard', 'Anne Arundel']
adjacency['Harford'] = ['Baltimore', 'Cecil']
adjacency['Cecil'] = ['Harford', 'Kent']
adjacency['Montgomery'] = ['Frederic', 'Howard,', "Prince George's"]
adjacency['Howard'] = ['Frederick', 'Carroll', 'Baltimore', 'Montgomery', "Prince George's", 'Ann Arundel']
adjacency['Baltimore City'] = ['Baltimore', 'Ann Arundel']
adjacency['Kent'] = ['Cecil', "Queen Anne's"]
adjacency["Prince George's"] = ['Montgomery']
adjacency['Ann Arundel'] = ['Howard', 'Baltimore', 'Baltimore City', "Prince George's", 'Calvert']
adjacency["Queen Anne's"] = ['Kent', 'Talbot', 'Caroline']
adjacency['Talbot'] = ["Queen Anne's", 'Caroline', 'Dorchester']
adjacency['Calvert'] = ["Prince George's", 'Ann Arundel', 'Charles', "St. Mary's"]
adjacency['Charles'] = ["Prince George's", 'Calvert', "St. Mary's"]
adjacency['Dorchester'] = ['Talbot', 'Caroline', 'Wicomico']
adjacency['Wicomico'] = ['Dorchester', 'Worcester', 'Somerset']
adjacency["St. Mary's"] = ['Charles', 'Calvert']
adjacency['Somerset'] = ['Wicomico', 'Worcester']
adjacency['Worcester'] = ['Wicomico', 'Somerset']



adjacency['New Castle'] = ['Kent']
adjacency['Kent'] = ['New Castle', 'Sussex']
adjacency['Sussex'] = ['Kent']



adjacency[''] = ['']
adjacency[''] = ['']
adjacency[''] = ['']
adjacency[''] = ['']
adjacency[''] = ['']
adjacency[''] = ['']


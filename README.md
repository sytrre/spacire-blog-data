# Spacire Shopify Data - Public Store API

**🔄 Auto-updates every 30 minutes | 💷 All prices in GBP**

## 📊 Quick Access - Shopify Format JSON Data

### Main Data Files
| Data Type | Description | Direct URL | Format |
|-----------|-------------|------------|---------|
| **Products** | Complete product catalog | [products.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/products.json) | Shopify Standard |
| **Collections** | All collections | [collections.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections.json) | Shopify Standard |
| **Blogs** | All blog posts | [blogs.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/blogs.json) | Blog JSON |
| **Data Index** | Complete file listing | [data_index.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/data_index.json) | Index JSON |

### Collection-Specific Product Files
Each collection has its own product file. Examples:
- [Blackout Curtains](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/blackout-curtains_products.json)
- [Sleep Masks](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/sleep-masks_products.json)
- [Weighted Blankets](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/weighted-blankets_products.json)

View all collection files in the [collections/](./collections) directory

## 🚀 API Usage Examples

### Fetch All Products
```bash
curl https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/products.json
```

### Fetch Specific Collection Products
```bash
curl https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/sleep-masks_products.json
```

### JavaScript/React Example
```javascript
// Fetch all products
fetch('https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/products.json')
  .then(res => res.json())
  .then(data => {
    console.log(`Found ${data.products.length} products`);
    data.products.forEach(product => {
      console.log(`${product.title} - £${product.variants[0].price}`);
    });
  });
```

## 📦 Data Structure

### Product Object
- **Full HTML descriptions** in `body_html` field
- **Complete variant data** with inventory, SKUs, barcodes
- **Multiple images** with dimensions and alt text
- **Product options** (size, color, etc.)
- **Pricing in GBP** with compare-at prices
- **SEO metadata** included

### Collection Object
- Collection metadata and descriptions
- Product count
- Sort order and publishing info
- Image with dimensions

## 🔄 Update Schedule
- **Every 30 minutes**: Full data sync
- **Manual trigger**: Available via GitHub Actions
- **Format**: Matches Shopify's standard JSON structure
- **Currency**: GBP (British Pounds)

## 📝 Data Index
For a complete list of all available files and URLs, check:
- [data_index.txt](./data_index.txt) - Human readable index
- [data_index.json](./data_index.json) - Machine readable index

## 🛠️ Technical Details
- **Source**: Shopify GraphQL API
- **Pagination**: 250 items per request
- **Repository**: Public for direct URL access
- **No authentication required** for read access

## 📧 Integration Support
This data is perfect for:
- External websites and applications
- Price comparison tools
- Inventory management systems
- Content management systems
- Mobile applications
- Analytics and reporting tools

---

Last updated: $(date)

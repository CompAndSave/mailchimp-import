'use strict';

const axios = require('axios');
const { timer } = require('cas-common-lib');
const MongoDB = require("./mongodb");

// determine how many api calls to run async to MailChimp API servers
// Mailchimp limits the maximum of concurrent api calls to 10
//
const MAX_CONCURRENT_CALLS = 10;


// decode google_analytic field to get the year, quarter, month, promoNum (campaign) and segment data
// e.g., 2020_jan2_sku_most  - Year: 2020, Month: 1, Quarter: 1, promoNum: 2, segment: sku_most
//
function decodeGaKey(gaKey) {
  let regex = new RegExp(`^([\\d]{4})_([\\w]{3})(\\d)_(.*)$`);
  let match = gaKey.match(regex);
  let result = {};

  result.year = Number(match[1]);
  result.month = match[2];
  result.promo_num = Number(match[3]);
  result.segment = match[4];

  switch (result.month) {
    case "jan":
      result.month = 1;
      result.quarter = 1;
      break;
    case "feb":
      result.month = 2;
      result.quarter = 1;
      break;
    case "mar":
      result.month = 3;
      result.quarter = 1;
      break;
    case "apr":
      result.month = 4;
      result.quarter = 2;
      break;
    case "may":
      result.month = 5;
      result.quarter = 2;
      break;
    case "jun":
      result.month = 6;
      result.quarter = 2;
      break;
    case "jul":
      result.month = 7;
      result.quarter = 3;
      break;
    case "aug":
      result.month = 8;
      result.quarter = 3;
      break;
    case "sep":
      result.month = 9;
      result.quarter = 3;
      break;
    case "oct":
      result.month = 10;
      result.quarter = 4;
      break;
    case "nov":
      result.month = 11;
      result.quarter = 4;
      break;
    case "dec":
      result.month = 12;
      result.quarter = 4;
      break;
    default: throw new Error(`invalid-month-${result.month}`);
  }

  return result;
}

// variateParams is an object like following
// {
//   type: "variate-parent",
//   child_ids: ["123546asd", "789486asd"]
// }
//
// {
//   type: "variate-child",
//   parent_id: "ajkajs123"
// }
//
function formatCampaignReportData(site, campaignData, reportData, variateParams) {
  delete reportData.bounces.syntax_errors;
  delete reportData.opens.opens_total;
  delete reportData.opens.last_open;
  delete reportData.clicks.clicks_total;
  delete reportData.clicks.unique_clicks;
  delete reportData.clicks.last_click;

  let res = {
    _id: reportData.id,
    site: site,
    type: variateParams ? variateParams.type : campaignData.type,
    year: campaignData.year,
    quarter: campaignData.quarter,
    month: campaignData.month,
    promo_num: campaignData.promo_num,
    segment: campaignData.segment,
    emails_sent: reportData.emails_sent,
    abuse_reports: reportData.abuse_reports,
    unsubscribed: reportData.unsubscribed,
    bounces: reportData.bounces,
    opens: reportData.opens,
    clicks: reportData.clicks
  };

  if (variateParams && variateParams.child_ids) { res.child_ids = variateParams.child_ids; }
  else if (variateParams && variateParams.parent_id) { res.parent_id = variateParams.parent_id; }

  return res;
}

// Multiple site and audience list support in one MailChimp account
//
// params - mcAudienceId object example
//    {
//       site1: YOUR_1ST_AUDIENCE_LIST_ID,
//       site2: YOUR_2ND_AUDIENCE_LIST_ID,
//       site3: YOUR_3RD_AUDIENCE_LIST_ID
//    }
// 
class MailChimpImport {
  constructor (mcAudienceId, mcUserName, mcApiKey, campaignDataCollectionName, reportDataCollectionName, mcApiUrl = "https://us2.api.mailchimp.com/3.0/") {
    this.campaignData = new MongoDB(campaignDataCollectionName);
    this.campaignReport = new MongoDB(reportDataCollectionName);
    this.mcApiUrl = mcApiUrl;
    this.mcUserName = mcUserName;
    this.mcApiKey = mcApiKey;
    this.audienceId = mcAudienceId;
    this.header = {
      auth: {
        username: mcUserName,
        password: mcApiKey
      }
    };
  }

  // Methods to fetch data from MailChimp
  // ------------------------ Start ---------------------------------- //
  //
  async getCampaignData(site, count, sinceSendTime, sortField) {
    return Promise.resolve((await axios.get(`${this.mcApiUrl}campaigns?&count=${count}&since_send_time=${sinceSendTime}&list_id=${this.audienceId[site]}&sort_field=${sortField}`, this.header)).data);
  }

  async getCampaignContentData(campaignId) {
    return Promise.resolve((await axios.get(`${this.mcApiUrl}campaigns/${campaignId}/content`, this.header)));
  }

  async getReportData(campaignId) {
    return Promise.resolve((await axios.get(`${this.mcApiUrl}reports/${campaignId}`, {
      ...this.header,
      campaign_id: campaignId
    })));
  }

  // Save fetched MailChimp data to database
  // type param: either "campaignData" or "campaignReport"
  //
  async importData(type, importArr) {
    let replaceOneArr = [];
    importArr.forEach(element => {
      replaceOneArr.push({
        replaceOne: {
          filter: { _id: element._id },
          replacement: element,
          upsert: true
        }
      });
    });

    return type === "campaignData" ? Promise.resolve(await this.campaignData.allBulkUnOrdered(replaceOneArr)) :
           type === "campaignReport" ? Promise.resolve(await this.campaignReport.allBulkUnOrdered(replaceOneArr)) :
           Promise.reject(`invalid-type-${type}`);
  }

  // Fetch on regular and variate campaign type from MailChimp
  // Campaign type other than regular and variate will be filtered out and return at invalidCampaign
  // Multiple sites are supported
  //
  // params:
  //  - site
  //  - startTime (start from when campaign sent date) - E.g., 2020-01-01
  //  - count (no. of campaigns data to be fetched) - default: 300
  //  - sort (sort by field) - default: "send_time"
  //
  async fetchCampaignData (site, startTime, count = 300, sort = "send_time") {
    let data = await this.getCampaignData(site, count, startTime, sort);
    let campaigns = data.campaigns;
  
    let campaignData = [], invalidCampaign = [], loopCount = 0, loopCampaignData = [], content = [];
    for (let i = 0; i < campaigns.length; ++i) {
      if (campaigns[i].type === "regular" || campaigns[i].type === "variate") {
        let decodedGaKey = decodeGaKey(campaigns[i].tracking.google_analytics);
        content.push(this.getCampaignContentData(campaigns[i].id));

        let tempData = {
          _id: campaigns[i].id,
          list_id: this.audienceId[site],
          title: campaigns[i].settings.title,
          type: campaigns[i].type,
          year: decodedGaKey.year,
          quarter: decodedGaKey.quarter,
          month: decodedGaKey.month,
          promo_num: decodedGaKey.promo_num,
          segment: decodedGaKey.segment
        };

        if (campaigns[i].type === "regular") {
          tempData.subject_line = campaigns[i].settings.subject_line;
          tempData.preview_text = campaigns[i].settings.preview_text;
          tempData.send_time = campaigns[i].send_time;
          tempData.google_analytics = `${campaigns[i].id}-${campaigns[i].tracking.google_analytics}`;
        }
        else {
          tempData.google_analytics = campaigns[i].tracking.google_analytics;
          tempData.variate_settings = campaigns[i].variate_settings;
        }

        loopCampaignData.push(tempData);
      }
      else {
        invalidCampaign.push({
          id: campaigns[i].id,
          type: campaigns[i].type
        });
      }

      if (loopCount >= MAX_CONCURRENT_CALLS - 1 || i >= campaigns.length - 1) {
        content = await Promise.all(content);
        for (let j = 0; j < loopCampaignData.length; ++j) {
          if (loopCampaignData[j].type === "regular") { loopCampaignData[j].content = content[j].data.html; }
          else {
            let variateContents = [];
            content[j].data.variate_contents.forEach(element => variateContents.push(element.html));
            loopCampaignData[j].variate_contents = variateContents;
          }
        }
        campaignData.push(...loopCampaignData);
        loopCampaignData = [], loopCount = 0, content = [];
      }
      else { ++loopCount; }
    }

    return Promise.resolve({
      campaignData: campaignData,
      invalidCampaign: invalidCampaign
    });
  }

  // Fetch report data from MailChimp based on the campaign at db
  // Multiple sites are supported
  //
  // params:
  //  - site
  //  - startTime (formatted YYYY-MM-DD) (if not provided, all campaign data of the specified site will be pull)
  //  - campaignId (if MailChimp campaign id is provided, only fetch the report data of that id, site and startTime param will be ignored)
  //
  async fetchReportData(site, startTime, campaignId) {
    let reportData = [], projection = { _id: 1, type: 1, variate_settings: 1, year: 1, quarter: 1, month: 1, promo_num: 1, segment: 1 };
    let year = Number(startTime.substring(0, 4)), month = Number(startTime.substring(5, 7));
    let docs = typeof campaignId === "string" ? await this.getCampaignDbData({ _id: campaignId }, projection) :
               typeof startTime === "string" ? await this.getCampaignDbData({ list_id: this.audienceId[site], $or: [{ year: { $gt: year }}, { year: { $gte: year }, month: { $gte: month }}]}, projection) :
                 await this.getAllCampaignDbDatabySite(site, projection);

    let mcReportData = [], loopData = [], loopCount = 0;
    for (let i = 0; i < docs.length; ++i) {
      loopData.push(this.getReportData(docs[i]._id));
      if (loopCount >= MAX_CONCURRENT_CALLS - 1 || i >= docs.length - 1) {
        loopData = await Promise.all(loopData);
        loopData.forEach(item => mcReportData.push(item.data));
        loopCount = 0; loopData = [];
        await timer.timeout(500);   // Add 0.5 second timeout to prevent over the concurrent api call limit
      }
      else { ++loopCount; }
    }

    for (let i = 0; i < docs.length; ++i) {  
      if (docs[i].type === "variate") {
        let childIds = [];
        docs[i].variate_settings.combinations.forEach(item => childIds.push(item.id));
        reportData.push(formatCampaignReportData(site, docs[i], mcReportData[i], {
          type: "variate-parent",
          child_ids: childIds
        }));
  
        for (let j = 0; j < childIds.length; ++j) {
          reportData.push(formatCampaignReportData(site, docs[i], (await this.getReportData(childIds[j])).data, {
            type: "variate-child",
            parent_id: docs[i]._id
          }));
        }
      }
      else {
        reportData.push(formatCampaignReportData(site, docs[i], mcReportData[i]));
      }
    }

    return Promise.resolve(reportData);
  }
  // ------------------------ End ---------------------------------- //


  // Methods to get and write data to MongoDB which stores MailChimp campaign data
  // ------------------------ Start ---------------------------------- //
  //
  async getAllCampaignDbDatabySite(site, projection, sort, pagination) {
    return Promise.resolve((await this.campaignData.getData("campaignData", { list_id: this.audienceId[site] }, false, projection, sort, pagination, false)));
  }

  async getCampaignDbData(query, projection, sort, pagination) {
    return Promise.resolve((await this.campaignData.getData("campaignData", query, false, projection, sort, pagination, false)));
  }

  async getReportDbData(query, projection, sort, pagination) {
    return Promise.resolve((await this.campaignReport.getData("campaignReport", query, false, projection, sort, pagination, false)));
  }
  // ------------------------ End ---------------------------------- //
}

module.exports = MailChimpImport;